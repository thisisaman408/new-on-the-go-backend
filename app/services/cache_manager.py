"""
Advanced Multi-Layer Cache Manager for RSS News Aggregation
Implements sophisticated caching strategies for production-scale news delivery
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
import logging
from dataclasses import dataclass
from collections import defaultdict

from app.utils.redis_client import get_redis_client, RedisClient
from app.models.article import Article
from app.models.source import NewsSource
from app.database import AsyncSessionLocal
from app.config import settings
from sqlalchemy import select, func, and_, desc

logger = logging.getLogger(__name__)

@dataclass
class CacheConfig:
    """Cache layer configuration with production-ready defaults"""
    content_hash_ttl: int = 86400  # 24 hours
    topic_cache_ttl: int = 10800   # 3 hours  
    recency_cache_ttl: int = 3600  # 1 hour
    source_perf_ttl: int = 1800    # 30 minutes
    digest_cache_ttl: int = 7200   # 2 hours
    analytics_ttl: int = 600       # 10 minutes
    max_articles_per_cache: int = 200
    cache_warming_enabled: bool = True

class CacheAnalytics:
    """Track cache performance metrics"""
    
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.writes = 0
        self.invalidations = 0
        self.warming_operations = 0
        self.start_time = datetime.utcnow()
    
    def record_hit(self):
        self.hits += 1
    
    def record_miss(self):
        self.misses += 1
    
    def record_write(self):
        self.writes += 1
    
    def record_invalidation(self):
        self.invalidations += 1
    
    def record_warming(self):
        self.warming_operations += 1
    
    @property
    def hit_ratio(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        return {
            'hit_ratio_percent': round(self.hit_ratio, 2),
            'total_hits': self.hits,
            'total_misses': self.misses,
            'total_writes': self.writes,
            'total_invalidations': self.invalidations,
            'warming_operations': self.warming_operations,
            'uptime_seconds': uptime,
            'operations_per_second': (self.hits + self.misses + self.writes) / max(uptime, 1)
        }

class AdvancedCacheManager:
    """
    Production-grade multi-layer cache manager implementing:
    - Layer 1: Content Hash Cache (deduplication & fast lookup)
    - Layer 2: Topic-Based Cache (categorized content)
    - Layer 3: Recency Cache (time-bucketed content)
    - Layer 4: Source Performance Cache (RSS source metrics)
    - Layer 5: Pre-computed Digests Cache (WhatsApp/API delivery)
    """
    
    def __init__(self, config: Optional[CacheConfig] = None):
        self.redis: RedisClient = get_redis_client()
        self.config = config or CacheConfig()
        self.analytics = CacheAnalytics()
        self.cache_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
    
    # === LAYER 1: CONTENT HASH CACHE ===
    
    async def cache_article_by_hash(self, article: Article) -> bool:
        """Cache article by content hash for fast deduplication"""
        try:
            article_data = {
                'id': article.id,
                'title': article.title,
                'url': article.url,
                'source_name': article.source_name,
                'primary_topic': article.primary_topic,
                'discovered_at': article.discovered_at.isoformat() if article.discovered_at is not None else None,
                'cached_at': datetime.utcnow().isoformat()
            }
            
            content_hash = getattr(article, "content_hash", None)
            if isinstance(content_hash, str) and content_hash:
                success = self.redis.cache_article_by_hash(
                    content_hash, 
                    article_data, 
                    self.config.content_hash_ttl
                )
                if success:
                    self.analytics.record_write()
                return success
            else:
                logger.error("Article content_hash is missing or not a string; cannot cache article by hash.")
                return False
            
        except Exception as e:
            logger.error(f"Error caching article by hash: {e}")
            return False
    
    async def get_article_by_hash(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached article by content hash"""
        try:
            article_data = self.redis.get_cached_article(content_hash)
            
            if article_data:
                self.analytics.record_hit()
            else:
                self.analytics.record_miss()
            
            return article_data
            
        except Exception as e:
            logger.error(f"Error getting article by hash: {e}")
            self.analytics.record_miss()
            return None
    
    # === LAYER 2: TOPIC-BASED CACHE ===
    
    async def warm_topic_caches(self, priority_topics: Optional[List[str]] = None) -> Dict[str, int]:
        """Pre-populate topic caches with recent articles"""
        async with self.cache_locks["topic_warming"]:
            try:
                self.analytics.record_warming()
                
                if not priority_topics:
                    priority_topics = await self._get_active_topics()
                
                results = {}
                
                async with AsyncSessionLocal() as session:
                    for topic in priority_topics:
                        try:
                            # Get recent articles for this topic
                            result = await session.execute(
                                select(Article.id)
                                .filter(
                                    Article.primary_topic == topic,
                                    Article.discovered_at >= datetime.utcnow() - timedelta(hours=6)
                                )
                                .order_by(desc(Article.discovered_at))
                                .limit(self.config.max_articles_per_cache)
                            )
                            
                            article_ids = [row.id for row in result.fetchall()]
                            
                            if article_ids:
                                self.redis.cache_articles_by_topic(
                                    topic, 
                                    article_ids, 
                                    self.config.topic_cache_ttl
                                )
                                results[topic] = len(article_ids)
                                self.analytics.record_write()
                            else:
                                results[topic] = 0
                                
                        except Exception as e:
                            logger.error(f"Error warming cache for topic {topic}: {e}")
                            results[topic] = 0
                
                logger.info(f"Warmed topic caches: {results}")
                return results
                
            except Exception as e:
                logger.error(f"Topic cache warming failed: {e}")
                return {}
    
    async def get_articles_by_topic(self, topic: str, limit: int = 50) -> List[int]:
        """Get cached articles by topic with fallback to database"""
        try:
            # Try cache first
            cached_ids = self.redis.get_articles_by_topic(topic)
            
            if cached_ids:
                self.analytics.record_hit()
                return cached_ids[:limit]
            
            # Cache miss - fetch from database and cache
            self.analytics.record_miss()
            return await self._fetch_and_cache_topic_articles(topic, limit)
            
        except Exception as e:
            logger.error(f"Error getting articles by topic: {e}")
            return []
    
    # === LAYER 3: RECENCY CACHE ===
    
    async def warm_recency_caches(self) -> Dict[str, int]:
        """Pre-populate recency caches with time-bucketed articles"""
        async with self.cache_locks["recency_warming"]:
            try:
                self.analytics.record_warming()
                
                time_buckets = {
                    '1h': datetime.utcnow() - timedelta(hours=1),
                    '6h': datetime.utcnow() - timedelta(hours=6),
                    '24h': datetime.utcnow() - timedelta(hours=24)
                }
                
                results = {}
                
                async with AsyncSessionLocal() as session:
                    for bucket_name, cutoff_time in time_buckets.items():
                        try:
                            result = await session.execute(
                                select(Article.id)
                                .filter(Article.discovered_at >= cutoff_time)
                                .order_by(desc(Article.discovered_at))
                                .limit(self.config.max_articles_per_cache)
                            )
                            
                            article_ids = [row.id for row in result.fetchall()]
                            
                            success = self.redis.cache_articles_by_recency(
                                bucket_name, 
                                article_ids, 
                                self.config.recency_cache_ttl
                            )
                            
                            if success:
                                results[bucket_name] = len(article_ids)
                                self.analytics.record_write()
                            else:
                                results[bucket_name] = 0
                                
                        except Exception as e:
                            logger.error(f"Error warming recency cache {bucket_name}: {e}")
                            results[bucket_name] = 0
                
                logger.info(f"Warmed recency caches: {results}")
                return results
                
            except Exception as e:
                logger.error(f"Recency cache warming failed: {e}")
                return {}
    
    async def get_articles_by_recency(self, time_bucket: str, limit: int = 50) -> List[int]:
        """Get cached articles by time bucket"""
        try:
            cached_ids = self.redis.get_articles_by_recency(time_bucket)
            
            if cached_ids:
                self.analytics.record_hit()
                return cached_ids[:limit]
            
            self.analytics.record_miss()
            # Could implement fallback to database here if needed
            return []
            
        except Exception as e:
            logger.error(f"Error getting articles by recency: {e}")
            return []
    
    # === LAYER 4: SOURCE PERFORMANCE CACHE ===
    
    async def cache_source_performance_metrics(self) -> Dict[str, int]:
        """Cache performance metrics for all RSS sources"""
        try:
            cached_sources = 0
            
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(NewsSource)
                    .filter(NewsSource.enabled.is_(True)) #type: ignore
                )
                
                sources = result.scalars().all()
                
                for source in sources:
                    try:
                        metrics = {
                            'reliability_score': source.reliability_score,
                            'success_rate': source.success_rate,
                            'avg_response_time_ms': source.avg_response_time_ms,
                            'total_articles_collected': source.total_articles_collected,
                            'consecutive_failures': source.consecutive_failures,
                            'last_successful_poll_at': source.last_successful_poll_at.isoformat() if source.last_successful_poll_at is not None else None,
                            'is_healthy': source.is_healthy,
                            'cached_at': datetime.utcnow().isoformat()
                        }
                        
                        success = self.redis.cache_source_performance(
                            int(source.id),  # type: ignore
                            metrics, 
                            self.config.source_perf_ttl
                        )
                        
                        if success:
                            cached_sources += 1
                            self.analytics.record_write()
                            
                    except Exception as e:
                        logger.error(f"Error caching metrics for source {source.id}: {e}")
            
            logger.info(f"Cached performance metrics for {cached_sources} sources")
            return {'sources_cached': cached_sources}
            
        except Exception as e:
            logger.error(f"Source performance caching failed: {e}")
            return {'sources_cached': 0}
    
    async def get_top_performing_sources(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top performing sources from cache"""
        try:
            top_sources = []
            
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(NewsSource.id)
                    .filter(NewsSource.enabled.is_(True)) #type: ignore
                    .order_by(desc(getattr(NewsSource, "reliability_score")))
                    .limit(limit * 2)  # Get more to filter cached ones
                )
                
                source_ids = [row.id for row in result.fetchall()]
                
                for source_id in source_ids:
                    cached_metrics = self.redis.get_source_performance(source_id)
                    if cached_metrics:
                        cached_metrics['source_id'] = source_id
                        top_sources.append(cached_metrics)
                        self.analytics.record_hit()
                        
                        if len(top_sources) >= limit:
                            break
                    else:
                        self.analytics.record_miss()
            
            # Sort by reliability score
            top_sources.sort(key=lambda x: x.get('reliability_score', 0), reverse=True)
            return top_sources[:limit]
            
        except Exception as e:
            logger.error(f"Error getting top performing sources: {e}")
            return []
    
    # === LAYER 5: PRE-COMPUTED DIGESTS ===
    
    async def cache_news_digest(self, digest_type: str, digest_content: Dict[str, Any]) -> bool:
        """Cache pre-computed news digests"""
        try:
            digest_with_meta = {
                **digest_content,
                'generated_at': datetime.utcnow().isoformat(),
                'digest_type': digest_type,
                'article_count': len(digest_content.get('articles', [])),
                'cache_version': '1.0'
            }
            
            success = self.redis.cache_news_digest(
                digest_type, 
                digest_with_meta, 
                self.config.digest_cache_ttl
            )
            
            if success:
                self.analytics.record_write()
            
            return success
            
        except Exception as e:
            logger.error(f"Error caching news digest: {e}")
            return False
    
    async def get_news_digest(self, digest_type: str) -> Optional[Dict[str, Any]]:
        """Get cached news digest"""
        try:
            digest = self.redis.get_news_digest(digest_type)
            
            if digest:
                self.analytics.record_hit()
            else:
                self.analytics.record_miss()
            
            return digest
            
        except Exception as e:
            logger.error(f"Error getting news digest: {e}")
            self.analytics.record_miss()
            return None
    
    # === CACHE MANAGEMENT & ANALYTICS ===
    
    async def invalidate_caches_for_new_articles(self, articles: List[Article]) -> Dict[str, int]:
        """Smart cache invalidation when new articles arrive"""
        try:
            invalidated = {
                'topics': 0,
                'recency': 0,
                'digests': 0
            }
            
            # Get unique topics from new articles
            topics_to_invalidate = set()
            for article in articles:
                if article.primary_topic is not None:
                    topics_to_invalidate.add(article.primary_topic)
            
            # Invalidate topic caches
            for topic in topics_to_invalidate:
                if self.redis.invalidate_topic_cache(topic):
                    invalidated['topics'] += 1
                    self.analytics.record_invalidation()
            
            # Invalidate recency caches (they need refresh with new articles)
            for time_bucket in ['1h', '6h', '24h']:
                if self.redis.delete(f"recency:{time_bucket}:articles"):
                    invalidated['recency'] += 1
                    self.analytics.record_invalidation()
            
            # Invalidate current hour digest (will be regenerated)
            current_hour = datetime.utcnow().strftime('%Y%m%d_%H')
            digest_keys = [f"digest:morning:{current_hour}", f"digest:evening:{current_hour}"]
            for key in digest_keys:
                if self.redis.delete(key):
                    invalidated['digests'] += 1
                    self.analytics.record_invalidation()
            
            logger.info(f"Smart invalidation completed: {invalidated}")
            return invalidated
            
        except Exception as e:
            logger.error(f"Cache invalidation failed: {e}")
            return {'topics': 0, 'recency': 0, 'digests': 0}
    
    async def warm_all_caches(self) -> Dict[str, Any]:
        """Warm all cache layers"""
        if not self.config.cache_warming_enabled:
            return {'status': 'disabled'}
        
        try:
            start_time = datetime.utcnow()
            
            # Warm caches in parallel
            tasks = [
                self.warm_topic_caches(),
                self.warm_recency_caches(),
                self.cache_source_performance_metrics()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            end_time = datetime.utcnow()
            
            warming_stats = {
                'status': 'completed',
                'topic_warming': results[0] if not isinstance(results[0], Exception) else {'error': str(results[0])},
                'recency_warming': results[1] if not isinstance(results[1], Exception) else {'error': str(results[1])},
                'source_performance': results[2] if not isinstance(results[2], Exception) else {'error': str(results[2])},
                'warming_time_seconds': (end_time - start_time).total_seconds(),
                'timestamp': end_time.isoformat()
            }
            
            logger.info(f"Cache warming completed: {warming_stats}")
            return warming_stats
            
        except Exception as e:
            logger.error(f"Cache warming failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def get_cache_analytics(self) -> Dict[str, Any]:
        """Get comprehensive cache analytics"""
        try:
            redis_analytics = self.redis.get_cache_analytics()
            manager_analytics = self.analytics.get_stats()
            
            return {
                'manager_stats': manager_analytics,
                'redis_stats': redis_analytics,
                'cache_config': {
                    'content_hash_ttl': self.config.content_hash_ttl,
                    'topic_cache_ttl': self.config.topic_cache_ttl,
                    'recency_cache_ttl': self.config.recency_cache_ttl,
                    'max_articles_per_cache': self.config.max_articles_per_cache,
                    'warming_enabled': self.config.cache_warming_enabled
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting cache analytics: {e}")
            return {'error': str(e)}
    
    # === PRIVATE HELPER METHODS ===
    
    async def _get_active_topics(self, limit: int = 15) -> List[str]:
        """Get most active topics from recent articles"""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Article.primary_topic, func.count(Article.id).label('count'))
                    .filter(
                        Article.discovered_at >= datetime.utcnow() - timedelta(hours=24),
                        Article.primary_topic.isnot(None)
                    )
                    .group_by(Article.primary_topic)
                    .order_by(desc(func.count(Article.id)))
                    .limit(limit)
                )
                
                return [row.primary_topic for row in result.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting active topics: {e}")
            return ['technology', 'business', 'politics', 'general']  # Fallback
    
    async def _fetch_and_cache_topic_articles(self, topic: str, limit: int) -> List[int]:
        """Fetch articles from database and cache them"""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Article.id)
                    .filter(Article.primary_topic == topic)
                    .order_by(desc(Article.discovered_at))
                    .limit(limit)
                )
                
                article_ids = [row.id for row in result.fetchall()]
                
                if article_ids:
                    # Cache for future requests
                    self.redis.cache_articles_by_topic(
                        topic, 
                        article_ids, 
                        self.config.topic_cache_ttl
                    )
                    self.analytics.record_write()
                
                return article_ids
                
        except Exception as e:
            logger.error(f"Error fetching and caching topic articles: {e}")
            return []

# Global cache manager instance
cache_manager = AdvancedCacheManager()

# Convenience functions for easy integration
async def warm_all_caches() -> Dict[str, Any]:
    """Warm all cache layers"""
    return await cache_manager.warm_all_caches()

async def invalidate_caches_for_articles(articles: List[Article]) -> Dict[str, int]:
    """Invalidate caches when new articles arrive"""
    return await cache_manager.invalidate_caches_for_new_articles(articles)

async def get_cached_articles_smart(
    topic: Optional[str] = None,
    time_bucket: Optional[str] = None,
    limit: int = 50
) -> List[int]:
    """Smart article retrieval using multiple cache layers"""
    
    if time_bucket:
        # Try recency cache first
        article_ids = await cache_manager.get_articles_by_recency(time_bucket, limit)
        if article_ids:
            return article_ids
    
    if topic:
        # Try topic cache
        article_ids = await cache_manager.get_articles_by_topic(topic, limit)
        if article_ids:
            return article_ids
    
    # No cache hits
    return []

def get_cache_stats() -> Dict[str, Any]:
    """Get cache performance statistics"""
    return cache_manager.get_cache_analytics()
