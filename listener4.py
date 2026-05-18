import asyncio
from email import message
import discord
from discord.ext import commands
import os
import httpx
import zmq
import time
import boto3
import concurrent.futures
import random
from pymongo import MongoClient
from dotenv import load_dotenv
from io import BytesIO
from bson import ObjectId
import datetime
import re
from botocore.config import Config
import unicodedata
# from performance import update_throughput

load_dotenv()

# # # --- 1. CONFIGURATION ---
TOKENS = [
    os.getenv("DISCORD_TOKEN_1"),
    os.getenv("DISCORD_TOKEN_2"),
    os.getenv("DISCORD_TOKEN_3"),
    os.getenv("DISCORD_TOKEN_4"),
    os.getenv("DISCORD_TOKEN_5"),
    os.getenv("DISCORD_TOKEN_6"),
    os.getenv("DISCORD_TOKEN_7"),
    os.getenv("DISCORD_TOKEN_8"),
    os.getenv("DISCORD_TOKEN_9"),
    os.getenv("DISCORD_TOKEN_10")
]

# AWS S3 Credentials (formerly MinIO)
ACCESS_KEY = os.getenv('ACCESS_KEY') # Your IAM Access Key
SECRET_KEY = os.getenv('SECRET_KEY')     # Your IAM Secret Key
BUCKET_NAME = os.getenv('BUCKET_NAME') # Removed trailing space
REGION = os.getenv('REGION') # Mumbai Region

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "discord_data_5"
COLLECTION_NAME = "messages"

BATCH_SIZE = 170
BATCH_TIMEOUT = 4

s3_config = Config(
    connect_timeout=30, 
    read_timeout=60,      # Increase read timeout to 60s
    retries={'max_attempts': 5} # Auto-retry on glitch
)

# --- 2. SHARED RESOURCES & CLIENTS ---
# ZeroMQ Setup
zmq_context = zmq.Context()
zmq_socket = zmq_context.socket(zmq.PUSH)
zmq_socket.bind("tcp://127.0.0.1:5555")

# Thread Pool for IO-bound AWS S3 uploads
upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Clients
# Updated to native AWS S3 (removed endpoint_url)
s3_client = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
    config=s3_config
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
messages_collection = db[COLLECTION_NAME]

# Persistent HTTPX client
http_client = httpx.Client(
    timeout=httpx.Timeout(30.0, read=60.0), 
    follow_redirects=True, 
    limits=httpx.Limits(max_connections=25, max_keepalive_connections=10)
)

# --- 3. HELPER FUNCTIONS ---
def sanitize_filename(filename):
    """Prevents InvalidObjectName by stripping emojis and symbols."""
    filename = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode('ascii')
    filename = re.sub(r'[^a-zA-Z0-9.\-_]', '_', filename)
    return filename if filename else "attachment"

def upload_to_s3(url, message_id, filename, content_type, custom_metadata):
    """Replaced upload_to_minio with AWS S3 logic."""
    try:
        response = http_client.get(url)
        if response.status_code == 200:
            file_data = BytesIO(response.content)
            safe_filename = sanitize_filename(filename)
            unique_name = f"{message_id}_{safe_filename}"

            # Upload to AWS with Public Read ACL for MongoDB Compass visibility
            s3_client.upload_fileobj(
                file_data, 
                BUCKET_NAME, 
                unique_name,
                ExtraArgs={
                    "ContentType": content_type, 
                    "Metadata": custom_metadata,
                    "ACL": "public-read" 
                },
                
            )
            # Return the standard AWS S3 URL
            return f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{unique_name}"
    except Exception as e:
        print(f"❌ AWS S3 Upload Failed: {e}")
    
    return None

def process_and_update_db(mongo_id_str, data, item, item_type):
    """
    Handles S3 upload and updates the specific attachment entry in MongoDB.
    Expects 'item' to be a valid attachment dictionary.
    """
    try:
        mongo_id = ObjectId(mongo_id_str)
    except Exception as e:
        print(f"❌ ID Conversion Error: {e}")
        return

    # Helper for S3 metadata compliance
    def clean_s3_meta(value):
        if not value: return "Unknown"
        return str(value).encode("ascii", "ignore").decode("ascii").strip() or "Unknown"
    
    # 1. Map item_type to the correct MongoDB array field
    mapping = {
        "image": "images",
        "audio": "audio_files",
        "video": "videos"
    }
    field_name = mapping.get(item_type, "other_files")

    # 2. Upload to S3
    custom_meta = {
        "guild-id": str(data.get("guild_id", "DM")),
        "guild-name": clean_s3_meta(data.get("guild_name", "DM")),
        "author-name": clean_s3_meta(data.get("author_name", "Unknown")),
        "timestamp": str(data.get("event_time")),
        "message-id": str(data.get("message_id")),
        "mongodb-id": str(mongo_id)
    }
    
    aws_url = upload_to_s3(
        item["url"], 
        data["message_id"], 
        item.get("filename", "file"), 
        item.get("type", "application/octet-stream"),
        custom_meta
    )

    if not aws_url:
        return # Skip DB update if upload failed

    # 3. Update MongoDB
    try:
        # Unpack ObjectId for internal metadata tracking
        unpacked_metadata = {
            "stored_at": str(mongo_id.generation_time),
            "machine_pid_hex": mongo_id.binary[4:9].hex(),
            "counter_hex": mongo_id.binary[9:12].hex(),
        }
        
        # Use array_filters to target the exact attachment by its original URL
        update_payload = {
            "$set": {
                "obj_id_unpacking": unpacked_metadata,
                f"{field_name}.$[elem].s3_url": aws_url
            }
        }
        filters = [{"elem.url": item["url"]}]

        messages_collection.update_one(
            {"_id": mongo_id}, 
            update_payload, 
            array_filters=filters
        )
        
        # print(f"✅ Mongo updated with S3 URL for {item_type}: {item.get('filename')}")
            
    except Exception as e:
        print(f"❌ Mongo Update Error: {e}")

# # # --- 4. THE BATCH WORKER (Consumer) ---

def db_worker():
    worker_context = zmq.Context()
    receiver = worker_context.socket(zmq.PULL)
    receiver.connect("tcp://127.0.0.1:5555")
    
    print("🚀 ZMQ DB Worker (AWS S3 Enabled) started...")
    batch = []
    last_flush_time = time.time()
    
    # Ensure index exists for performance and data integrity
    messages_collection.create_index("message_id", unique=True)

    while True:
        try:
            try:
                # Non-blocking receive to allow timeout-based flushing
                data = receiver.recv_json(flags=zmq.NOBLOCK)
            except zmq.Again:
                data = None
            
            if data:
                
                # data["_id"] = ObjectId(data["_id"]) # Convert back to ObjectId for MongoDB
                # Generate the primary key here instead of in on_message
                data["_id"] = ObjectId()
                batch.append(data)

            current_time = time.time()
            # Flush batch if size limit reached or timeout occurred
            if len(batch) >= BATCH_SIZE or (batch and current_time - last_flush_time >= BATCH_TIMEOUT):
                try:
                     # Insert batch (duplicates will be ignored via ordered=False)
                    messages_collection.insert_many(batch, ordered=False)
                except Exception:
                    # BulkWriteErrors (duplicates) are ignored as intended
                    pass 

                for msg_data in batch:
                    real_id_str = str(msg_data["_id"])
                    
                    # 1. Process attachments ONLY if they exist
                    if msg_data.get("has_attachments"):
                        # Define categories to iterate through
                        media_categories = {
                            "image": "images",
                            "audio": "audio_files",
                            "video": "videos",
                            "other": "other_files"
                        }
                        
                        for item_type, field in media_categories.items():
                            for item in msg_data.get(field, []):
                                upload_executor.submit(
                                    process_and_update_db, 
                                    real_id_str, 
                                    msg_data, 
                                    item, 
                                    item_type
                                )
                    
                    # 2. Text-only messages don't need process_and_update_db 
                    # because they have no S3 work or array updates to perform.
                
                print(f"✅ Stored {len(batch)} messages. Media tasks queued.")
                batch = []
                last_flush_time = current_time
            
            if not data:
                time.sleep(0.1) # Prevent CPU spiking when queue is empty

        except Exception as e:
            print(f"⚠️ Worker Loop Error: {e}")
            time.sleep(1)

# # # --- 5. MULTI-ACCOUNT CLIENT CLASS ---
# (Remains identical to original functionality)
class MultiAccountClient(discord.Client):
    async def on_ready(self):
        print(f"✅ Account Active: {self.user}")
        self.loop.create_task(self.catch_up_historical())

    async def catch_up_historical(self):
        for guild in self.guilds:
            for channel in guild.text_channels:
                last_entry = messages_collection.find_one(
                    {"channel_id": str(channel.id)},
                    sort=[("event_time", -1)]
                )
                
                after_obj = discord.Object(id=int(last_entry["message_id"])) if last_entry else None
                try:
                    async for message in channel.history(limit=100, after=after_obj, oldest_first=True):
                        await self.on_message(message)
                        await asyncio.sleep(random.uniform(5.0, 8.0))
                except Exception:
                    continue

    async def on_message(self, message):
        if message.author == self.user:
            return
        # update_throughput()

        channel = message.channel
        member = message.author
        
        replied_to_name = None
        if message.reference and message.reference.resolved:
            if hasattr(message.reference.resolved, "author"):
                replied_to_name = str(message.reference.resolved.author)

        is_thread = isinstance(channel, discord.Thread)
        parent = channel.parent if is_thread else None
        attachments = message.attachments
        has_media = len(attachments) > 0
        # Prepare Data
        data = {
            # "_id": str(ObjectId()), # Stringify for ZMQ/JSON Producer (on_message): 
            # Generated  This was a CPU-heavy string conversion.
            "message_id": str(message.id),
            "scraped_by_account": str(self.user),
            "author_id": str(member.id),
            "author_name": str(member),
            "guild_id": str(message.guild.id) if message.guild else None,
            "guild_name": message.guild.name if message.guild else "DM",
            "channel_id": str(channel.id),
            "channel_name": getattr(channel, "name", "DirectMessage"),
            "channel_type": str(channel.type),
            "content": message.content,
            "category_name": str(getattr(channel.category, 'name', 'None')) if (not is_thread and hasattr(channel, 'category')) 
                             else str(getattr(parent.category, 'name', 'None')) if (is_thread and parent and hasattr(parent, 'category')) 
                             else "None",
            "replied_to_author": replied_to_name,
            "is_bot": member.bot,
            "is_thread": is_thread,
            "thread_id": str(channel.id) if is_thread else None,
            "parent_channel_id": str(parent.id) if parent else None,
            "message_type": str(message.type),
            "event_time": message.created_at.isoformat(),
            "processed_at": datetime.datetime.utcnow().isoformat(),
            "is_edited": message.edited_at is not None,
            "edited_timestamp": message.edited_at.isoformat() if message.edited_at else None,
            "has_attachments": has_media,
            "images": [],
            "audio_files": [],
            "other_files": [],
            "videos": []
        }

        if has_media:
            for a in attachments:
                if not a.content_type: continue
                media_item = {"url": a.url, "type": a.content_type, "filename": a.filename}            
                if "image" in a.content_type:
                    data["images"].append(media_item)
                elif "audio" in a.content_type:
                    data["audio_files"].append(media_item)
                elif "video" in a.content_type:
                    # You likely missed this list in your original producer code
                    data["videos"].append(media_item)
                else:
                # Catch-all for everything else (docs, zips, exe, etc.)
                    data["other_files"].append(media_item)
        
        
        # zmq_socket.send_json(data, flags=zmq.NOBLOCK)
        try:
        # We use send_json but add flags to ensure it doesn't wait if the buffer is full
           zmq_socket.send_json(data, flags=zmq.NOBLOCK)
        except zmq.Again:
        # If the worker is too busy, we log it instead of lagging the bot
           print(f"⚠️ ZMQ Buffer Full: Dropping message {message.id} to prevent  lag.")