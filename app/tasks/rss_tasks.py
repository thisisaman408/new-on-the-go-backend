"""
Background tasks for RSS collection, content processing, and maintenance
Integrates with existing RSSCollector, ContentProcessor, and ArticleDeduplicator
FIXED: Asyncio event loop conflicts resolved for Celery workers
"""

import asyncio
import nest_asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

from celery import Task
from app.tasks.celery_app import celery_app
from app.services.rss_collector import collect_rss_articles, RSSCollector
from app.services.content_processor import process_articles
from app.services.deduplicator import deduplicate_articles
from app.utils.redis_client import get_redis_client
from app.database import AsyncSessionLocal
from app.models.source import NewsSource
from sqlalchemy import select, func

# Apply nest_asyncio to allow nested event loops in Celery workers
nest_asyncio.apply()

logger = logging.getLogger(__name__)

class CallbackTask(Task):
    """Base task with enhanced error handling and monitoring"""
    
    def on_success(self, retval, task_id, args, kwargs):
        """Task success callback"""
        logger.info(f"Task {self.name} [{task_id}] succeeded: {retval}")
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Task failure callback"""
        logger.error(f"Task {self.name} [{task_id}] failed: {exc}")
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Task retry callback"""
        logger.warning(f"Task {self.name} [{task_id}] retrying: {exc}")

def run_async_safely(coro):
    """
    Safely run async coroutine in Celery worker context
    Handles event loop conflicts between Celery and asyncio
    """
    try:
        # First, try to get the current event loop
        try: 
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, we need nest_asyncio to handle nested calls
                # DO NOT use asyncio.run() here - it creates a new loop!
                import nest_asyncio
                nest_asyncio.apply()
                
                # Create a task and run it
                task = loop.create_task(coro)
                
                # For running loops, we need to use a different approach
                # Use asyncio.ensure_future and wait for completion
                future = asyncio.ensure_future(coro)
                
                # Wait for the future to complete using run_until_complete
                return loop.run_until_complete(future)
                
                return future.result()
            else:
                # Loop exists but not running - safe to use run_until_complete
                return loop.run_until_complete(coro)
                
        except RuntimeError as e:
            if "no running event loop" in str(e).lower() or "no current event loop" in str(e).lower():
                # No event loop exists, create a new one
                logger.debug("No event loop found, creating new one")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(coro)
                finally:
                    # Clean up the loop
                    try:
                        # Cancel any remaining tasks
                        pending = asyncio.all_tasks(loop)
                        for task in pending:
                            task.cancel()
                        
                        # Wait for cancellation to complete
                        if pending:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception as cleanup_error:
                        logger.warning(f"Loop cleanup warning: {cleanup_error}")
                    finally:
                        loop.close()
                        # Remove the loop from thread local storage
                        asyncio.set_event_loop(None)
            else:
                raise
                
    except Exception as e:
        logger.error(f"run_async_safely failed: {e}")
        # Last resort: try with asyncio.run (only if no loop exists)
        try:
            asyncio.get_event_loop()
            # Loop exists, can't use asyncio.run
            raise
        except RuntimeError:
            # No loop, safe to use asyncio.run
            logger.debug("Using asyncio.run as last resort")
            return asyncio.run(coro)

@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.collect_all_rss_sources')
def collect_all_rss_sources(self, max_concurrent: int = 5) -> Dict[str, Any]:
    """
    Scheduled task: Collect articles from all RSS sources
    Runs every 15 minutes via Celery Beat
    """
    task_start = datetime.utcnow()
    logger.info(f"Starting RSS collection task - ID: {self.request.id}")
    
    try:
        # Run async RSS collection with safe event loop handling
        stats = run_async_safely(collect_rss_articles(max_concurrent=max_concurrent))
        
        # Cache results in Redis
        cache_key = f"rss_collection:{task_start.strftime('%Y%m%d_%H%M')}"
        redis_client = get_redis_client()
        redis_client.setex(cache_key, 3600, str(stats))  # Cache for 1 hour
        
        # Log results
        processing_time = (datetime.utcnow() - task_start).total_seconds()
        logger.info(
            f"RSS collection completed - Sources: {stats['sources_processed']}, "
            f"Articles: {stats['articles_collected']}, Time: {processing_time:.2f}s"
        )
        
        # Schedule content processing if we collected articles
        if stats['articles_collected'] > 0:
            process_articles_background.apply_async(countdown=300)  # Process in 5 minutes
        
        return {
            **stats,
            'task_id': self.request.id,
            'processing_time_seconds': processing_time,
            'scheduled_content_processing': stats['articles_collected'] > 0
        }
        
    except Exception as exc:
        logger.error(f"RSS collection task failed: {exc}")
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))

@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.collect_single_source')
def collect_single_source(self, source_id: int) -> Dict[str, Any]:
    """
    Collect articles from a single RSS source
    Used for priority sources or manual triggers
    """
    logger.info(f"Collecting from single source ID: {source_id}")
    
    try:
        async def _collect_single():
            async with AsyncSessionLocal() as session:
                # Get source
                result = await session.execute(select(NewsSource).filter(NewsSource.id == source_id))
                source = result.scalar_one_or_none()
                
                if not source:
                    raise ValueError(f"Source {source_id} not found")
                
                # Collect from this source
                async with RSSCollector() as collector:
                    return await collector._collect_from_source(source)
        
        result = run_async_safely(_collect_single())
        
        logger.info(f"Single source collection completed: {result}")
        return result
        
    except Exception as exc:
        logger.error(f"Single source collection failed: {exc}")
        raise self.retry(exc=exc, countdown=60)

@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.process_articles_background')
def process_articles_background(self, batch_size: int = 50) -> Dict[str, Any]:
    """
    Background content processing task - FIXED VERSION
    Enhances articles with metadata and deduplication
    """
    logger.info("Starting background content processing")
    
    try:
        # Run content processing with safe event loop handling
        stats = run_async_safely(process_articles())
        
        logger.info(
            f"Content processing completed - Processed: {stats['articles_processed']}, "
            f"Enhanced: {stats.get('enhanced_articles', 0)}"
        )
        
        return {
            **stats,
            'task_id': self.request.id
        }
        
    except Exception as exc:
        logger.error(f"Content processing task failed: {exc}")
        raise self.retry(exc=exc, countdown=120)

@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.deduplicate_articles_background')
def deduplicate_articles_background(self, days_back: int = 3) -> Dict[str, Any]:
    """
    Background deduplication task
    Runs daily to clean up duplicate articles
    """
    logger.info(f"Starting background deduplication (last {days_back} days)")
    
    try:
        async def _run_deduplication():
            # Run hash-based deduplication
            hash_stats = await deduplicate_articles("hash", days_back=days_back)
            
            # Run title similarity deduplication
            title_stats = await deduplicate_articles("title", days_back=days_back)
            
            return hash_stats, title_stats
        
        # Run with safe event loop handling
        hash_stats, title_stats = run_async_safely(_run_deduplication())
        
        total_removed = hash_stats['duplicates_removed'] + title_stats['duplicates_removed']
        
        logger.info(f"Deduplication completed - Removed: {total_removed} duplicates")
        
        return {
            'total_duplicates_removed': total_removed,
            'hash_based_removed': hash_stats['duplicates_removed'],
            'title_similarity_removed': title_stats['duplicates_removed'],
            'task_id': self.request.id
        }
        
    except Exception as exc:
        logger.error(f"Deduplication task failed: {exc}")
        raise self.retry(exc=exc, countdown=300)

@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.health_check_sources')
def health_check_sources(self) -> Dict[str, Any]:
    """
    Health check task for RSS sources
    Monitors source performance and enables/disables sources
    """
    logger.info("Running RSS sources health check")
    
    try:
        async def _health_check():
            async with AsyncSessionLocal() as session:
                # Get sources with high failure rates
                result = await session.execute(
                    select(NewsSource)
                    .filter(NewsSource.enabled.is_(True))  # type: ignore[attr-defined]
                )
                sources = list(result.scalars().all())
                
                health_report = {
                    'total_sources': len(sources),
                    'healthy_sources': 0,
                    'problematic_sources': 0,
                    'disabled_sources': 0
                }
                
                for source in sources:
                    failure_rate = (getattr(source, 'failed_polls', 0) or 0) / max(getattr(source, 'total_polls', 1) or 1, 1)
                    
                    if failure_rate > 0.7 and bool(source.consecutive_failures >= 5):
                        # Disable problematic source
                        source.enabled = False
                        health_report['disabled_sources'] += 1
                        logger.warning(f"Disabled problematic source: {source.name}")
                    elif failure_rate > 0.5:
                        health_report['problematic_sources'] += 1
                    else:
                        health_report['healthy_sources'] += 1
                
                await session.commit()
                return health_report
        
        health_report = run_async_safely(_health_check())
        
        logger.info(f"Health check completed: {health_report}")
        return health_report
        
    except Exception as exc:
        logger.error(f"Health check task failed: {exc}")
        raise self.retry(exc=exc, countdown=300)

@celery_app.task(bind=True, base=CallbackTask)
def manual_source_trigger(self, source_names: List[str]) -> Dict[str, Any]:
    """
    Manually trigger collection for specific sources
    Useful for high-priority sources or debugging
    """
    logger.info(f"Manual trigger for sources: {source_names}")
    
    try:
        async def _manual_collect():
            results = []
            async with AsyncSessionLocal() as session:
                for source_name in source_names:
                    result = await session.execute(
                        select(NewsSource).filter(NewsSource.name == source_name)
                    )
                    source = result.scalar_one_or_none()
                    
                    if source:
                        async with RSSCollector() as collector:
                            source_result = await collector._collect_from_source(source)
                            results.append(source_result)
                    else:
                        results.append({
                            'source_name': source_name,
                            'error': 'Source not found',
                            'articles_collected': 0
                        })
            
            return results
        
        results = run_async_safely(_manual_collect())
        
        total_articles = sum(r.get('articles_collected', 0) for r in results)
        logger.info(f"Manual collection completed - Total articles: {total_articles}")
        
        return {
            'results': results,
            'total_articles_collected': total_articles,
            'task_id': self.request.id
        }
        
    except Exception as exc:
        logger.error(f"Manual source trigger failed: {exc}")
        raise self.retry(exc=exc, countdown=60)

# Utility functions for task monitoring
def get_task_status(task_id: str) -> Dict[str, Any]:
    """Get status of a Celery task"""
    result = celery_app.AsyncResult(task_id)
    return {
        'task_id': task_id,
        'status': result.status,
        'result': result.result if result.ready() else None,
        'traceback': result.traceback if result.failed() else None
    }

def get_active_tasks() -> List[Dict[str, Any]]:
    """Get list of active tasks"""
    inspect = celery_app.control.inspect()
    active_tasks = inspect.active()
    
    if active_tasks:
        all_tasks = []
        for worker, tasks in active_tasks.items():
            for task in tasks:
                all_tasks.append({
                    'worker': worker,
                    'task_id': task['id'],
                    'name': task['name'],
                    'args': task['args'],
                    'kwargs': task['kwargs']
                })
        return all_tasks
    
    return []

# For testing
if __name__ == '__main__':
    print("Testing RSS tasks...")
    print(f"Celery app: {celery_app}")
    print(f"Available tasks: {list(celery_app.tasks.keys())}")
    print("✅ RSS tasks configured successfully!")
