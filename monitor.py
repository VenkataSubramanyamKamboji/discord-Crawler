import psutil
import time
from datetime import datetime

def run_slim_monitor(interval=60):
    print(f"🚀 Logging started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Format: [Time] CPU | RAM (used) | NET Sent | NET Recv | DISK (free)\n")
    
    try:
        while True:
            # 1. Capture Time
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            # 2. CPU Usage (interval=1 ensures accuracy)
            cpu_pct = psutil.cpu_percent(interval=1)
            
            # 3. RAM Usage
            mem = psutil.virtual_memory()
            ram_pct = mem.percent
            ram_used_gb = mem.used / (1024**3)
            
            # 4. Network Sent
            net_sent_mb = psutil.net_io_counters().bytes_sent / (1024**2)
            net_recv_mb = psutil.net_io_counters().bytes_recv / (1024**2)
            
            # 5. DISK Usage (ADDED)
            disk = psutil.disk_usage('/')
            disk_pct = disk.percent
            disk_free_gb = disk.free / (1024**3)
            
            # 6. Combined Single-Line Output
            print(f"[{timestamp}] CPU: {cpu_pct:.1f}% | RAM: {ram_pct:.1f}% ({ram_used_gb:.2f}GB used) | "
                  f"NET Sent: {net_sent_mb:.2f}MB  NET Recv: {net_recv_mb:.2f}MB  | DISK: {disk_pct:.1f}% ({disk_free_gb:.1f}GB free)")
            
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped.")

if __name__ == "__main__":
    run_slim_monitor()