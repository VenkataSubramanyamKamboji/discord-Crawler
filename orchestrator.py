import asyncio
import threading
import time
import random
import sys
import os
import discord
import httpx
import boto3
import datetime

# Import your specific classes and resources from your worker file
from listener4 import MultiAccountClient, db_worker, TOKENS, zmq_context, upload_executor

# 1-to-1 Mapping: Slot Index -> Specific Duplicate Token
RESERVE_MAP = {
    0: os.getenv("RESERVE_TOKEN_1"),
    1: os.getenv("RESERVE_TOKEN_2"),
    2: os.getenv("RESERVE_TOKEN_3"),
    3: os.getenv("RESERVE_TOKEN_4"),
    4: os.getenv("RESERVE_TOKEN_5"),
    5: os.getenv("RESERVE_TOKEN_6"),
    6: os.getenv("RESERVE_TOKEN_7"), 
    7: os.getenv("RESERVE_TOKEN_8"),
    8: os.getenv("RESERVE_TOKEN_9"),
    9: os.getenv("RESERVE_TOKEN_10")
}

messages_in_current_batch = 0


class IngestionOrchestrator:
    def __init__(self, reserve_map):
        self.fleet = {}  # Tracks account instances: {index: client_object}
        self.reserve_map = reserve_map
        self.is_running = True

    def is_internet_up(self):
        try:
            # Using a fast timeout to check connectivity
            httpx.get("https://8.8.8.8", timeout=3.0)
            return True
        except:
            return False

    def handle_replacement(self, index):
        """Logic for 1-to-1 Hot-Swap: Replaces a banned slot with its specific twin"""
        # 1. Direct Dictionary Lookup (Fast O(1) speed) for duplicate token.
        new_token = self.reserve_map.get(index)

        if not new_token:
            print(f"🛑 Orchestrator: No specific backup available for Slot {index + 1}.")
            # it Marks as BANNED_ACCOUNT so the health monitor ignores this slot permanently
            self.fleet[index] = "BANNED_ACCOUNT"
            return

        print(f"♻️ Slot {index + 1}: Twin found. Initiating hot-swap...")

        # 2. Safety Delete from reserve so it's not reused
        del self.reserve_map[index]

        # 3. Resource & IP Safety Wait (Human Mimicry)
        # Essential for 8GB RAM to let garbage collection clear the old 'client'
        wait_time = random.uniform(20, 30) 
        print(f"⏳ Waiting {round(wait_time, 1)}s for network/RAM safety...")
        time.sleep(wait_time)

        # 4. Re-launch the twin into the same slot index
        self.spawn_worker(new_token, index)

    def spawn_worker(self, token, index):
        """Orchestrator: Launches a single worker account in a monitored thread"""
        # Disable chunking to save 8GB RAM
        client = MultiAccountClient(chunk_guilds_at_startup=False)
        self.fleet[index] = client
        
        def run_worker():
            try:
                # Start the Discord connection
                client.run(token)
            except discord.errors.LoginFailure:
                # THIS IS THE BAN TRIGGER
                print(f"🚨 ALERT: Account {index + 1} is BANNED or the Token is Invalid!")
                # Mark this slot as banned so the monitor loop knows a swap is needed
                self.fleet[index] = "BANNED_ACCOUNT"

                # TRIGGER AUTO-REPLACEMENT
                self.handle_replacement(index)
                
                try:
                    # Clean up the event loop for the failed client
                    asyncio.run_coroutine_threadsafe(client.close(), client.loop)
                except:
                    pass
            except Exception as e:
                print(f"⚠️ Orchestrator: Worker {index + 1} stopped. Error: {e}")

        # daemon=True ensures threads close when the main script stops
        t = threading.Thread(target=run_worker, daemon=True)
        t.start()
        print(f"✅ Orchestrator: Worker {index + 1} launched successfully.")

    def start_system(self):
        """The 'Director' sequence for high-throughput ingestion"""
        print("🚀 Starting Ingestion System Orchestration...")

        # 1. Start the Storage Service (DB Worker) first
        db_thread = threading.Thread(target=db_worker, daemon=True)
        db_thread.start()
        print("🗄️ Orchestrator: Database Storage Worker active.")

        # 2. Launch all accounts with randomized delays (Orchestration)
        for i, token in enumerate(TOKENS):
            if token:
                self.spawn_worker(token, i)
                # Random Jitter to look human and stay under rate limits
                wait_time = random.uniform(5, 7)
                print(f"⏳ Orchestrator: Waiting {round(wait_time, 1)}s before next worker...")
                time.sleep(wait_time)

        # 3. The Monitor Loop (Self-Healing)
        try:
            while self.is_running:
                time.sleep(29) # Checks system health every 30 seconds
                # Filter to count ONLY truly active/connected clients
                actual_active = sum(1 for c in self.fleet.values() if hasattr(c, 'is_closed') and not c.is_closed())
                # --- PANIC SWITCH ---
                # Count current banned slots
                banned_count = list(self.fleet.values()).count("BANNED_ACCOUNT")

                if banned_count >= 3:
                    print(f"🛑 PANIC SWITCH TRIGGERED: {banned_count} bans detected.")
                    print("Shutting down immediately to save remaining accounts and prevent IP flagging.")
                    self.is_running = False
                    os._exit(1) 
                
                # Health Check Loop
                for i, client in self.fleet.items():
                    # 1. Skip accounts we know are permanently banned (no reserves left)
                    if client == "BANNED_ACCOUNT":
                        continue

                    # 2. Safety Check: Ensure 'client' is a Discord object before checking status
                    # This prevents AttributeError when a slot is marked with a string
                    if hasattr(client, 'is_closed') and client.is_closed():
                        print(f"🔄 Orchestrator: Worker {i + 1} is down. Checking network...")

                        # 3. Network-Safe Reattempt
                        if self.is_internet_up():
                            print(f"📡 Network OK. Attempting recovery for Worker {i + 1}...")
                            self.spawn_worker(TOKENS[i], i)
                        else:
                            print(f"📡 Network is still down. Recovery for Worker {i + 1} postponed.")

                # Report system-wide throughput stats
                print(f"📊 System Status | In-Batch: {messages_in_current_batch} | Active Workers: {actual_active}")

        except KeyboardInterrupt:
            print("\n🛑 Orchestrator: Emergency shutdown initiated...")
            self.is_running = False
            print("⏳ Waiting for pending AWS S3 uploads to finish...")
            upload_executor.shutdown(wait=True) 
            print("✅ All nodes closed. System offline.")

if __name__ == "__main__":
    # Pass the RESERVE_MAP into the Orchestrator
    director = IngestionOrchestrator(RESERVE_MAP)
    director.start_system()










