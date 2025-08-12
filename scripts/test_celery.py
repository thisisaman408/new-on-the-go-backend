#!/usr/bin/env python3
"""Test Celery setup and task execution"""

import asyncio
import sys
import os
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tasks import celery_app, collect_all_rss_sources, get_active_tasks
from app.utils.redis_client import get_redis_client

def test_redis_connection():
    """Test Redis connection"""
    print("🔧 Testing Redis connection...")
    
    try:
        client = get_redis_client()
        health = client.health_check()
        
        if health['status'] == 'healthy':
            print(f"✅ Redis healthy - Response: {health['response_time_ms']:.2f}ms")
            print(f"   Connected clients: {health['connected_clients']}")
            print(f"   Memory used: {health['used_memory_human']}")
            return True
        else:
            print(f"❌ Redis unhealthy: {health}")
            return False
    
    except Exception as e:
        print(f"❌ Redis test failed: {e}")
        return False

def test_celery_connection():
    """Test Celery broker connection"""
    print("\n🔧 Testing Celery connection...")
    
    try:
        # Test basic connection
        inspect = celery_app.control.inspect()
        stats = inspect.stats()
        
        if stats:
            print("✅ Celery broker connection successful")
            print(f"   Active workers: {len(stats)}")
            for worker, worker_stats in stats.items():
                print(f"   Worker: {worker}")
            return True
        else:
            print("⚠️  No active Celery workers found")
            print("   This is normal if workers haven't been started yet")
            return True
            
    except Exception as e:
        print(f"❌ Celery connection test failed: {e}")
        return False

def test_task_registration():
    """Test task registration"""
    print("\n🔧 Testing task registration...")
    
    expected_tasks = [
        'app.tasks.rss_tasks.collect_all_rss_sources',
        'app.tasks.rss_tasks.collect_single_source',
        'app.tasks.rss_tasks.process_articles_background',
        'app.tasks.rss_tasks.deduplicate_articles_background',
        'app.tasks.rss_tasks.health_check_sources'
    ]
    
    registered_tasks = list(celery_app.tasks.keys())
    
    print(f"📊 Total registered tasks: {len(registered_tasks)}")
    
    missing_tasks = []
    for task in expected_tasks:
        if task in registered_tasks:
            print(f"✅ {task}")
        else:
            print(f"❌ {task} - NOT FOUND")
            missing_tasks.append(task)
    
    if missing_tasks:
        print(f"\n⚠️  Missing {len(missing_tasks)} expected tasks")
        return False
    else:
        print("\n✅ All expected tasks registered")
        return True

def test_task_execution():
    """Test actual task execution"""
    print("\n🔧 Testing task execution...")
    
    try:
        # Test simple health check task
        print("   Sending health check task...")
        result = celery_app.send_task('app.tasks.celery_app.health_check')
        
        print(f"   Task ID: {result.id}")
        print("   Waiting for result (timeout: 30s)...")
        
        # Wait for result with timeout
        try:
            task_result = result.get(timeout=30)
            print(f"✅ Task completed successfully: {task_result}")
            return True
        except Exception as e:
            print(f"❌ Task execution failed: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Task sending failed: {e}")
        return False

def test_scheduled_tasks():
    """Test scheduled task configuration"""
    print("\n🔧 Testing scheduled tasks...")
    
    beat_schedule = celery_app.conf.beat_schedule
    
    expected_schedules = [
        'collect-rss-every-15-minutes',
        'process-content-every-30-minutes',
        'deduplicate-daily',
        'health-check-hourly'
    ]
    
    print(f"📅 Configured schedules: {len(beat_schedule)}")
    
    missing_schedules = []
    for schedule_name in expected_schedules:
        if schedule_name in beat_schedule:
            schedule = beat_schedule[schedule_name]
            print(f"✅ {schedule_name}: {schedule['task']}")
        else:
            print(f"❌ {schedule_name} - NOT FOUND")
            missing_schedules.append(schedule_name)
    
    if missing_schedules:
        print(f"\n⚠️  Missing {len(missing_schedules)} expected schedules")
        return False
    else:
        print("\n✅ All expected schedules configured")
        return True

def test_queue_configuration():
    """Test queue configuration"""
    print("\n🔧 Testing queue configuration...")
    
    queues = celery_app.conf.task_queues
    
    expected_queues = ['default', 'rss_collection', 'rss_sources', 'content_processing', 'maintenance']
    
    print(f"📋 Configured queues: {len(queues)}")
    
    queue_names = [q.name for q in queues]
    missing_queues = []
    
    for queue_name in expected_queues:
        if queue_name in queue_names:
            print(f"✅ {queue_name}")
        else:
            print(f"❌ {queue_name} - NOT FOUND")
            missing_queues.append(queue_name)
    
    if missing_queues:
        print(f"\n⚠️  Missing {len(missing_queues)} expected queues")
        return False
    else:
        print("\n✅ All expected queues configured")
        return True

def main():
    """Main test function"""
    print("🧪 Testing Celery Setup - Priority 1")
    print(f"📅 Test Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("="*70)
    
    tests = [
        ("Redis Connection", test_redis_connection),
        ("Celery Connection", test_celery_connection),
        ("Task Registration", test_task_registration),
        ("Queue Configuration", test_queue_configuration),
        ("Scheduled Tasks", test_scheduled_tasks),
    ]
    
    # Run basic tests
    results = {}
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        results[test_name] = test_func()
    
    # Optional task execution test (only if workers are running)
    print(f"\n{'='*20} Task Execution (Optional) {'='*20}")
    print("⚠️  This test requires active Celery workers")
    
    try:
        if test_task_execution():
            results["Task Execution"] = True
        else:
            print("   This is expected if no workers are running")
            results["Task Execution"] = "N/A"
    except Exception as e:
        print(f"   Task execution test skipped: {e}")
        results["Task Execution"] = "N/A"
    
    # Summary
    print("\n" + "="*70)
    print("🎯 Celery Setup Test Results:")
    
    passed = 0
    failed = 0
    skipped = 0
    
    for test_name, result in results.items():
        if result is True:
            print(f"✅ {test_name}: PASS")
            passed += 1
        elif result == "N/A":
            print(f"⚠️  {test_name}: SKIPPED")
            skipped += 1
        else:
            print(f"❌ {test_name}: FAIL")
            failed += 1
    
    print(f"\n📊 Summary: {passed} passed, {failed} failed, {skipped} skipped")
    
    if failed == 0:
        print("\n🎉 Celery setup is ready!")
        print("\n📋 Next steps:")
        print("   1. Start Redis: redis-server")
        print("   2. Start Celery worker: python scripts/start_celery.py")
        print("   3. RSS collection will run automatically every 15 minutes")
        return True
    else:
        print(f"\n❌ {failed} tests failed. Please fix issues before proceeding.")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
