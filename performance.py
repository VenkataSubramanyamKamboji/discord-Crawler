from pymongo import MongoClient
import time

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")

db = mongo_client["discord_data_5"]

messages_collection = db["messages"]

# -----------------------------
# INITIAL VALUES
# -----------------------------

# Existing DB count
initial_count = messages_collection.count_documents({})

# Start time
start_time = time.time()

# Previous runtime count
previous_runtime_count = 0

# Monitoring interval
INTERVAL = 10

# -----------------------------
# LIVE MONITORING LOOP
# -----------------------------
while True:

    # Total DB count
    total_count = messages_collection.count_documents({})

    # New messages during runtime
    runtime_count = total_count - initial_count

    # Total runtime
    elapsed_time = time.time() - start_time

    # TRUE average throughput
    avg_throughput = (
        runtime_count / elapsed_time
        if elapsed_time > 0 else 0
    )

    # Messages added during recent interval
    current_rate = (
        runtime_count - previous_runtime_count
    ) / INTERVAL

    print("\n========== LIVE PERFORMANCE REPORT ==========")

    print(f"New Messages Processed : {runtime_count}")

    print(f"Elapsed Time           : {elapsed_time:.2f} sec")

    print(f"Average Throughput     : {avg_throughput:.2f} msg/sec")

    print(f"Current Rate           : {current_rate:.2f} msg/sec")

    print("=============================================\n")

    # Update previous count
    previous_runtime_count = runtime_count

    # Wait interval
    time.sleep(INTERVAL)