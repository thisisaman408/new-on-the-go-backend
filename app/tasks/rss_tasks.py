"""
Background tasks for RSS collection, content processing, and maintenance
Integrates with existing RSSCollector, ContentProcessor, and ArticleDeduplicator
FIXED: Asyncio event loop conflicts resolved for Celery workers
ENHANCED: Advanced multi-layer caching integration for Priority 2
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
from app.models.article import Article
from sqlalchemy import select, func

# ENHANCED: Advanced caching integration
from app.services.cache_manager import cache_manager, invalidate_caches_for_articles


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
    Safely run async coroutine in Celery worker context - FIXED VERSION
    Handles event loop conflicts between Celery and asyncio
    """
    try:
        # Apply nest_asyncio to allow nested loops
        import nest_asyncio
        nest_asyncio.apply()
        
        # Get or create event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Use nest_asyncio's patched run_until_complete
        return loop.run_until_complete(coro)
        
    except Exception as e:
        logger.error(f"run_async_safely failed: {e}")
        raise


@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.collect_all_rss_sources')
def collect_all_rss_sources(self, max_concurrent: int = 5) -> Dict[str, Any]:
    """
    Scheduled task: Collect articles from all RSS sources with advanced caching
    Runs every 15 minutes via Celery Beat
    ENHANCED: Integrated with multi-layer cache management
    """
    task_start = datetime.utcnow()
    logger.info(f"Starting RSS collection task - ID: {self.request.id}")
    
    try:
        # Run async RSS collection with safe event loop handling
        stats = run_async_safely(collect_rss_articles(max_concurrent=max_concurrent))
        
        # Cache results in Redis (existing functionality)
        cache_key = f"rss_collection:{task_start.strftime('%Y%m%d_%H%M')}"
        redis_client = get_redis_client()
        redis_client.setex(cache_key, 3600, str(stats))  # Cache for 1 hour
        
        # ENHANCED: Advanced caching integration
        if stats['articles_collected'] > 0:
            # Get newly collected articles for cache invalidation
            async def _get_new_articles():
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(Article)
                        .filter(Article.discovered_at >= task_start)
                        .limit(stats['articles_collected'])
                    )
                    return list(result.scalars().all())
            
            # Get new articles and handle caching
            new_articles = run_async_safely(_get_new_articles())
            
            if new_articles:
                # Smart cache invalidation for new articles
                invalidation_stats = run_async_safely(invalidate_caches_for_articles(new_articles))
                stats['cache_invalidation'] = invalidation_stats
                logger.info(f"Cache invalidation completed: {invalidation_stats}")
                
                # Cache new articles by hash for fast deduplication
                cached_articles = 0
                for article in new_articles:
                    if run_async_safely(cache_manager.cache_article_by_hash(article)):
                        cached_articles += 1
                
                stats['articles_cached_by_hash'] = cached_articles
            
            # Warm all cache layers with new content
            cache_warming_stats = run_async_safely(cache_manager.warm_all_caches())
            stats['cache_warming'] = cache_warming_stats
            logger.info(f"Cache warming completed: {cache_warming_stats}")
            
            # Schedule content processing
            process_articles_background.apply_async(countdown=300)  # Process in 5 minutes
        
        # Log results
        processing_time = (datetime.utcnow() - task_start).total_seconds()
        logger.info(
            f"RSS collection completed - Sources: {stats['sources_processed']}, "
            f"Articles: {stats['articles_collected']}, Time: {processing_time:.2f}s"
        )
        
        # ENHANCED: Include cache performance metrics
        cache_analytics = cache_manager.get_cache_analytics()
        
        return {
            **stats,
            'task_id': self.request.id,
            'processing_time_seconds': processing_time,
            'scheduled_content_processing': stats['articles_collected'] > 0,
            'cache_analytics': cache_analytics.get('manager_stats', {}),
            'redis_analytics': cache_analytics.get('redis_stats', {})
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
    ENHANCED: Integrated with advanced caching
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
                    collection_result = await collector._collect_from_source(source)
                
                # ENHANCED: Cache source performance metrics
                if collection_result.get('articles_collected', 0) > 0:
                    source_metrics = {
                        'last_collection_at': datetime.utcnow().isoformat(),
                        'articles_collected': collection_result['articles_collected'],
                        'response_time_ms': collection_result.get('response_time_ms', 0),
                        'collection_success': True
                    }
                    cache_manager.redis.cache_source_performance(
                        source_id, source_metrics, 1800  # 30 minutes TTL
                    )
                
                return collection_result
        
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
    ENHANCED: Integrated with advanced caching
    """
    logger.info("Starting background content processing")
    
    try:
        # Run content processing with safe event loop handling
        stats = run_async_safely(process_articles())
        
        # ENHANCED: Refresh caches after content processing
        if stats.get('articles_processed', 0) > 0:
            # Warm topic caches with newly processed articles
            topic_warming_stats = run_async_safely(cache_manager.warm_topic_caches())
            stats['topic_cache_warming'] = topic_warming_stats
            
            # Update source performance cache with processing stats
            source_perf_stats = run_async_safely(cache_manager.cache_source_performance_metrics())
            stats['source_performance_caching'] = source_perf_stats
        
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
    ENHANCED: Integrated with cache invalidation
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
        
        # ENHANCED: Invalidate caches after deduplication
        if total_removed > 0:
            # Refresh all cache layers since articles were removed
            cache_refresh_stats = run_async_safely(cache_manager.warm_all_caches())
            logger.info(f"Cache refresh after deduplication: {cache_refresh_stats}")
        
        logger.info(f"Deduplication completed - Removed: {total_removed} duplicates")
        
        return {
            'total_duplicates_removed': total_removed,
            'hash_based_removed': hash_stats['duplicates_removed'],
            'title_similarity_removed': title_stats['duplicates_removed'],
            'task_id': self.request.id,
            'cache_refresh_triggered': total_removed > 0
        }
        
    except Exception as exc:
        logger.error(f"Deduplication task failed: {exc}")
        raise self.retry(exc=exc, countdown=300)


@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.health_check_sources')
def health_check_sources(self) -> Dict[str, Any]:
    """
    Health check task for RSS sources
    Monitors source performance and enables/disables sources
    ENHANCED: Integrated with source performance caching
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
                
                # ENHANCED: Update source performance cache after health check
                await cache_manager.cache_source_performance_metrics()
                
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
    ENHANCED: Integrated with caching
    """
    logger.info(f"Manual trigger for sources: {source_names}")
    
    try:
        async def _manual_collect():
            results = []
            collected_articles = []
            
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
                            
                            # ENHANCED: Get collected articles for caching
                            if source_result.get('articles_collected', 0) > 0:
                                articles_result = await session.execute(
                                    select(Article)
                                    .filter(Article.source_name == source_name)
                                    .order_by(Article.discovered_at.desc())
                                    .limit(source_result['articles_collected'])
                                )
                                collected_articles.extend(list(articles_result.scalars().all()))
                    else:
                        results.append({
                            'source_name': source_name,
                            'error': 'Source not found',
                            'articles_collected': 0
                        })
            
            # ENHANCED: Cache management for manually triggered sources
            if collected_articles:
                # Cache new articles and invalidate related caches
                invalidation_stats = await invalidate_caches_for_articles(collected_articles)
                
                # Cache articles by hash
                cached_count = 0
                for article in collected_articles:
                    if await cache_manager.cache_article_by_hash(article):
                        cached_count += 1
                
                return results, {
                    'cached_articles': cached_count,
                    'cache_invalidation': invalidation_stats
                }
            
            return results, {}
        
        results, cache_stats = run_async_safely(_manual_collect())
        
        total_articles = sum(r.get('articles_collected', 0) for r in results)
        logger.info(f"Manual collection completed - Total articles: {total_articles}")
        
        return {
            'results': results,
            'total_articles_collected': total_articles,
            'task_id': self.request.id,
            'cache_operations': cache_stats
        }
        
    except Exception as exc:
        logger.error(f"Manual source trigger failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


# ENHANCED: New cache management tasks
@celery_app.task(bind=True, base=CallbackTask, name='app.tasks.rss_tasks.warm_cache_layers')
def warm_cache_layers(self, layers: List[str] = []) -> Dict[str, Any]:
    """
    Manually warm specific cache layers
    Useful for cache management and performance optimization
    """
    logger.info(f"Warming cache layers: {layers or 'all'}")
    
    try:
        if not layers:
            # Warm all caches
            warming_stats = run_async_safely(cache_manager.warm_all_caches())
        else:
            # Warm specific layers
            warming_stats = {}
            for layer in layers:
                if layer == 'topics':
                    warming_stats['topics'] = run_async_safely(cache_manager.warm_topic_caches())
                elif layer == 'recency':
                    warming_stats['recency'] = run_async_safely(cache_manager.warm_recency_caches())
                elif layer == 'source_performance':
                    warming_stats['source_performance'] = run_async_safely(cache_manager.cache_source_performance_metrics())
        
        logger.info(f"Cache warming completed: {warming_stats}")
        return {
            'warming_stats': warming_stats,
            'task_id': self.request.id,
            'layers_warmed': layers or ['all']
        }
        
    except Exception as exc:
        logger.error(f"Cache warming task failed: {exc}")
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


# ENHANCED: Cache monitoring utilities
def get_cache_performance_summary() -> Dict[str, Any]:
    """Get comprehensive cache performance summary"""
    try:
        analytics = cache_manager.get_cache_analytics()
        redis_client = get_redis_client()
        
        return {
            'cache_manager_stats': analytics.get('manager_stats', {}),
            'redis_stats': analytics.get('redis_stats', {}),
            'cache_config': analytics.get('cache_config', {}),
            'redis_health': redis_client.health_check(),
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting cache performance summary: {e}")
        return {'error': str(e)}


# For testing
if __name__ == '__main__':
    print("Testing RSS tasks with advanced caching...")
    print(f"Celery app: {celery_app}")
    print(f"Available tasks: {list(celery_app.tasks.keys())}")
    
    # Test cache manager integration
    cache_stats = get_cache_performance_summary()
    print(f"Cache performance: {cache_stats}")
    
    print("✅ RSS tasks with advanced caching configured successfully!")
