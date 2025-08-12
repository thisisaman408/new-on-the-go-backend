#!/usr/bin/env python3
"""
Celery worker and beat startup script with environment detection
"""

import os
import sys
import subprocess
import signal
import time
from typing import List, Optional

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tasks.celery_app import celery_app
from app.utils.redis_client import get_redis_client

def check_redis_connection() -> bool:
    """Check if Redis is accessible"""
    try:
        client = get_redis_client()
        health = client.health_check()
        if health['status'] == 'healthy':
            print(f"✅ Redis connection healthy - Response time: {health['response_time_ms']:.2f}ms")
            return True
        else:
            print(f"❌ Redis unhealthy: {health}")
            return False
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False

def start_celery_worker(queues: Optional[List[str]] = None, 
                       concurrency: int = 4,
                       log_level: str = "info") -> subprocess.Popen:
    """Start Celery worker process"""
    
    if queues is None:
        queues = ['default', 'rss_collection', 'rss_sources', 'content_processing', 'maintenance']
    
    cmd = [
        'celery', '-A', 'app.tasks.celery_app:celery_app', 'worker',
        '--queues', ','.join(queues),
        '--concurrency', str(concurrency),
        '--loglevel', log_level,
        '--pool', 'solo'
    ]
    
    print(f"🚀 Starting Celery worker: {' '.join(cmd)}")
    
    return subprocess.Popen(cmd)

def start_celery_beat(log_level: str = "info") -> subprocess.Popen:
    """Start Celery beat scheduler"""
    
    cmd = [
        'celery', '-A', 'app.tasks.celery_app:celery_app', 'beat',
        '--loglevel', log_level,
        '--schedule', '/tmp/celerybeat-schedule',  # Linux/Mac
    ]
    
    # Windows compatibility
    if os.name == 'nt':
        cmd[-1] = 'celerybeat-schedule'
    
    print(f"📅 Starting Celery beat: {' '.join(cmd)}")
    
    return subprocess.Popen(cmd)

def start_celery_flower(port: int = 5555) -> subprocess.Popen:
    """Start Celery Flower monitoring (optional)"""
    
    cmd = [
        'celery', '-A', 'app.tasks.celery_app:celery_app', 'flower',
        '--port', str(port)
    ]
    
    print(f"🌸 Starting Celery Flower on port {port}")
    
    return subprocess.Popen(cmd)

def main():
    """Main startup function"""
    print("🚀 Starting Celery infrastructure...")
    
    # Check prerequisites
    if not check_redis_connection():
        print("❌ Redis connection required for Celery. Please start Redis first.")
        sys.exit(1)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Start Celery workers and beat scheduler')
    parser.add_argument('--worker-only', action='store_true', help='Start worker only')
    parser.add_argument('--beat-only', action='store_true', help='Start beat only')
    parser.add_argument('--with-flower', action='store_true', help='Start Flower monitoring')
    parser.add_argument('--concurrency', type=int, default=4, help='Worker concurrency')
    parser.add_argument('--log-level', default='info', help='Log level')
    parser.add_argument('--queues', nargs='+', help='Queues to process')
    
    args = parser.parse_args()
    
    processes = []
    
    try:
        # Start worker
        if not args.beat_only:
            worker_process = start_celery_worker(
                queues=args.queues,
                concurrency=args.concurrency,
                log_level=args.log_level
            )
            processes.append(('worker', worker_process))
        
        # Start beat scheduler
        if not args.worker_only:
            beat_process = start_celery_beat(log_level=args.log_level)
            processes.append(('beat', beat_process))
        
        # Start flower monitoring (optional)
        if args.with_flower:
            try:
                flower_process = start_celery_flower()
                processes.append(('flower', flower_process))
            except FileNotFoundError:
                print("⚠️  Flower not installed. Install with: pip install flower")
        
        if not processes:
            print("❌ No processes to start")
            return
        
        print(f"✅ Started {len(processes)} Celery processes")
        print("📊 Process status:")
        for name, process in processes:
            print(f"   {name}: PID {process.pid}")
        
        print("\n🎯 RSS collection will run every 15 minutes")
        print("📈 Content processing will run every 30 minutes")
        print("🧹 Deduplication will run daily at 2 AM UTC")
        print("\n⏹️  Press Ctrl+C to stop all processes")
        
        # Wait for processes and handle shutdown
        while True:
            time.sleep(1)
            # Check if any process died
            for name, process in processes:
                if process.poll() is not None:
                    print(f"❌ Process {name} (PID {process.pid}) died with code {process.returncode}")
                    return
    
    except KeyboardInterrupt:
        print("\n🛑 Shutting down Celery processes...")
        
        for name, process in processes:
            print(f"   Stopping {name} (PID {process.pid})...")
            process.terminate()
        
        # Wait for graceful shutdown
        time.sleep(5)
        
        # Force kill if necessary
        for name, process in processes:
            if process.poll() is None:
                print(f"   Force killing {name}...")
                process.kill()
        
        print("✅ All processes stopped")
    
    except Exception as e:
        print(f"❌ Error starting Celery: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
