"""
Cloud-migration ready Redis client wrapper
Handles connection pooling, failover, and environment detection
"""

import os
import redis
from redis.connection import ConnectionPool
# Add cast for type hinting and datetime for a bug fix
from typing import Optional, Any, Dict, List, Union, cast , Set
import logging
import json
from datetime import timedelta, datetime # Added datetime import
from collections import defaultdict
from app.config import settings

logger = logging.getLogger(__name__)

class RedisClient:
    """Enhanced Redis client with cloud migration support"""
    
    # Define client type hint for clarity
    client: redis.Redis

    def __init__(self, 
                 redis_url: Optional[str] = None,
                 max_connections: int = 50,
                 socket_timeout: int = 5,
                 socket_connect_timeout: int = 5,
                 retry_on_timeout: bool = True):
        
        self.redis_url = redis_url or self._get_redis_url()
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.retry_on_timeout = retry_on_timeout
        
        # Create connection pool
        self.pool = self._create_connection_pool()
        self.client = redis.Redis(connection_pool=self.pool)
        
        # Test connection
        self._test_connection()
    
    def _get_redis_url(self) -> str:
        """Get Redis URL with environment detection"""
        # Check for cloud Redis URLs first
        cloud_urls = [
            os.getenv('REDIS_URL'),
            os.getenv('REDISCLOUD_URL'),
            os.getenv('REDIS_TLS_URL'),
            os.getenv('ELASTICACHE_URL')
        ]
        
        for url in cloud_urls:
            if url:
                logger.info("Using cloud Redis connection")
                return url
        
        # Fallback to local configuration
        logger.info("Using local Redis connection")
        return settings.REDIS_URL
    
    def _create_connection_pool(self) -> ConnectionPool:
        """Create Redis connection pool with cloud-ready settings"""
        return ConnectionPool.from_url(
            self.redis_url,
            max_connections=self.max_connections,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_connect_timeout,
            retry_on_timeout=self.retry_on_timeout,
            health_check_interval=30,
            decode_responses=True
        )
    
    def _test_connection(self):
        """Test Redis connection and log status"""
        try:
            self.client.ping()
            logger.info(f"Redis connection established: {self._mask_url(self.redis_url)}")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            raise
    
    def _mask_url(self, url: str) -> str:
        """Mask credentials in Redis URL for logging"""
        import re
        return re.sub(r'://([^:]+):([^@]+)@', '://***:***@', url)
    
    # Core Redis operations with error handling and type casting for Pylance
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key"""
        try:
            return cast(Optional[str], self.client.get(key))
        except Exception as e:
            logger.error(f"Redis GET failed for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set key-value with optional expiration"""
        try:
            return cast(bool, self.client.set(key, value, ex=ex))
        except Exception as e:
            logger.error(f"Redis SET failed for key {key}: {e}")
            return False
    
    def setex(self, key: str, time: int, value: Any) -> bool:
        """Set key-value with expiration time"""
        try:
            return cast(bool, self.client.setex(key, time, value))
        except Exception as e:
            logger.error(f"Redis SETEX failed for key {key}: {e}")
            return False
    
    def delete(self, *keys: str) -> int:
        """Delete one or more keys"""
        try:
            return cast(int, self.client.delete(*keys))
        except Exception as e:
            logger.error(f"Redis DELETE failed for keys {keys}: {e}")
            return 0
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            # .exists() returns int, so we cast to int then bool
            return bool(cast(int, self.client.exists(key)))
        except Exception as e:
            logger.error(f"Redis EXISTS failed for key {key}: {e}")
            return False
    
    def expire(self, key: str, time: int) -> bool:
        """Set expiration time for key"""
        try:
            return cast(bool, self.client.expire(key, time))
        except Exception as e:
            logger.error(f"Redis EXPIRE failed for key {key}: {e}")
            return False
    
    def ttl(self, key: str) -> int:
        """Get time to live for key"""
        try:
            return cast(int, self.client.ttl(key))
        except Exception as e:
            logger.error(f"Redis TTL failed for key {key}: {e}")
            return -1
    
    # JSON operations for complex data
    
    def set_json(self, key: str, data: Dict[str, Any], ex: Optional[int] = None) -> bool:
        """Set JSON data with optional expiration"""
        try:
            json_str = json.dumps(data, default=str)
            return self.set(key, json_str, ex=ex)
        except Exception as e:
            logger.error(f"Redis SET_JSON failed for key {key}: {e}")
            return False
    
    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Get JSON data"""
        try:
            json_str = self.get(key)
            if json_str:
                return cast(Dict[str, Any], json.loads(json_str))
            return None
        except Exception as e:
            logger.error(f"Redis GET_JSON failed for key {key}: {e}")
            return None
    
    # List operations
    
    def lpush(self, key: str, *values: Any) -> int:
        """Push values to left of list"""
        try:
            return cast(int, self.client.lpush(key, *values))
        except Exception as e:
            logger.error(f"Redis LPUSH failed for key {key}: {e}")
            return 0
    
    def rpush(self, key: str, *values: Any) -> int:
        """Push values to right of list"""
        try:
            return cast(int, self.client.rpush(key, *values))
        except Exception as e:
            logger.error(f"Redis RPUSH failed for key {key}: {e}")
            return 0
    
    def lpop(self, key: str) -> Optional[str]:
        """Pop value from left of list"""
        try:
            return cast(Optional[str], self.client.lpop(key))
        except Exception as e:
            logger.error(f"Redis LPOP failed for key {key}: {e}")
            return None
    
    def lrange(self, key: str, start: int = 0, end: int = -1) -> List[str]:
        """Get range of list values"""
        try:
            return cast(List[str], self.client.lrange(key, start, end))
        except Exception as e:
            logger.error(f"Redis LRANGE failed for key {key}: {e}")
            return []
    
    # Set operations
    
    def sadd(self, key: str, *values: Any) -> int:
        """Add values to set"""
        try:
            return cast(int, self.client.sadd(key, *values))
        except Exception as e:
            logger.error(f"Redis SADD failed for key {key}: {e}")
            return 0
    
    def smembers(self, key: str) -> Set[str]:
        """Get all members of set"""
        try:
            return cast(Set[str], self.client.smembers(key))
        except Exception as e:
            logger.error(f"Redis SMEMBERS failed for key {key}: {e}")
            return set()
    
    # Hash operations
    
    def hset(self, key: str, field: str, value: Any) -> int:
        """Set hash field"""
        try:
            return cast(int, self.client.hset(key, field, value))
        except Exception as e:
            logger.error(f"Redis HSET failed for key {key}: {e}")
            return 0
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field"""
        try:
            return cast(Optional[str], self.client.hget(key, field))
        except Exception as e:
            logger.error(f"Redis HGET failed for key {key}: {e}")
            return None
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields"""
        try:
            return cast(Dict[str, str], self.client.hgetall(key))
        except Exception as e:
            logger.error(f"Redis HGETALL failed for key {key}: {e}")
            return {}
    
    # Cache-specific operations
    
    def cache_article_by_hash(self, content_hash: str, article_data: Dict[str, Any], ttl: int = 3600) -> bool:
        """Cache article data by content hash"""
        key = f"article:{content_hash}"
        return self.set_json(key, article_data, ex=ttl)
    
    def get_cached_article(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached article by content hash"""
        key = f"article:{content_hash}"
        return self.get_json(key)
    
    def cache_articles_by_topic(self, topic: str, article_ids: List[int], ttl: int = 1800):
        """Cache article IDs by topic"""
        key = f"topic:{topic}:articles"
        article_ids_str = [str(aid) for aid in article_ids]
        self.delete(key)  # Clear existing
        if article_ids_str:
            self.lpush(key, *article_ids_str)
            self.expire(key, ttl)
    
    def get_articles_by_topic(self, topic: str) -> List[int]:
        """Get cached article IDs by topic"""
        key = f"topic:{topic}:articles"
        article_ids_str = self.lrange(key)
        return [int(aid) for aid in article_ids_str if aid.isdigit()]
    
    def cache_rss_collection_stats(self, stats: Dict[str, Any], ttl: int = 3600) -> bool:
        """Cache RSS collection statistics"""
        # Fixed: Used datetime.now() which requires the datetime import
        key = f"rss:stats:{datetime.now().strftime('%Y%m%d_%H')}"
        return self.set_json(key, stats, ex=ttl)
    
    def get_recent_rss_stats(self) -> List[Dict[str, Any]]:
        """Get recent RSS collection statistics"""
        pattern = "rss:stats:*"
        keys = cast(List[str], self.client.keys(pattern))
        stats: List[Dict[str, Any]] = []
        for key in sorted(keys)[-24:]:  # Last 24 hours
            stat = self.get_json(key)
            if stat:
                stats.append(stat)
        return stats
    
    def cache_articles_by_recency(self, time_bucket: str, article_ids: List[int], ttl: int = 3600) -> bool:
        """Cache article IDs by time bucket (1h, 6h, 24h)"""
        key = f"recency:{time_bucket}:articles"
        try:
            article_ids_str = [str(aid) for aid in article_ids]
            self.delete(key)  # Clear existing
            if article_ids_str:
                self.lpush(key, *article_ids_str)
                self.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Error caching recency articles: {e}")
            return False
    
    def get_articles_by_recency(self, time_bucket: str) -> List[int]:
        """Get cached articles by time bucket"""
        key = f"recency:{time_bucket}:articles"
        try:
            article_ids_str = self.lrange(key)
            return [int(aid) for aid in article_ids_str if aid.isdigit()]
        except Exception as e:
            logger.error(f"Error getting recency articles: {e}")
            return []
    
    def cache_source_performance(self, source_id: int, metrics: Dict[str, Any], ttl: int = 1800) -> bool:
        """Cache RSS source performance metrics"""
        key = f"source_perf:{source_id}"
        try:
            return self.set_json(key, metrics, ex=ttl)
        except Exception as e:
            logger.error(f"Error caching source performance: {e}")
            return False
    
    def get_source_performance(self, source_id: int) -> Optional[Dict[str, Any]]:
        """Get cached source performance metrics"""
        key = f"source_perf:{source_id}"
        return self.get_json(key)
    
    def cache_news_digest(self, digest_type: str, content: Dict[str, Any], ttl: int = 7200) -> bool:
        """Cache pre-computed news digests"""
        key = f"digest:{digest_type}:{datetime.now().strftime('%Y%m%d_%H')}"
        return self.set_json(key, content, ex=ttl)
    
    def get_news_digest(self, digest_type: str) -> Optional[Dict[str, Any]]:
        """Get cached news digest (try current hour, then previous hour)"""
        for hour_offset in [0, 1]:
            target_time = datetime.now() - timedelta(hours=hour_offset)
            key = f"digest:{digest_type}:{target_time.strftime('%Y%m%d_%H')}"
            digest = self.get_json(key)
            if digest:
                return digest
        return None
    
    def invalidate_topic_cache(self, topic: str) -> bool:
        """Invalidate cache for specific topic"""
        key = f"topic:{topic}:articles"
        return bool(self.delete(key))
    
    def get_cache_analytics(self) -> Dict[str, Any]:
        """Get cache analytics and metrics"""
        try:
            info = cast(Dict[str, Any], self.client.info())
            
            # Get key counts by pattern
            patterns = {
                'articles': 'article:*',
                'topics': 'topic:*',
                'recency': 'recency:*',
                'source_perf': 'source_perf:*',
                'digests': 'digest:*',
                'rss_stats': 'rss:stats:*'
            }
            
            key_counts = {}
            total_keys = 0
            
            for cache_type, pattern in patterns.items():
                keys = cast(List[str], self.client.keys(pattern))
                key_counts[cache_type] = len(keys)
                total_keys += len(keys)
            
            return {
                'total_keys': total_keys,
                'key_counts_by_type': key_counts,
                'memory_usage': info.get('used_memory_human', 'Unknown'),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'cache_hit_rate': self._calculate_hit_rate(),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting cache analytics: {e}")
            return {'error': str(e)}
    
    def _calculate_hit_rate(self) -> float:
        """Calculate cache hit rate from Redis stats"""
        try:
            info = cast(Dict[str, Any], self.client.info())
            hits = info.get('keyspace_hits', 0)
            misses = info.get('keyspace_misses', 0)
            total = hits + misses
            return (hits / total * 100) if total > 0 else 0.0
        except Exception:
            return 0.0


    # Health and monitoring
    
    def health_check(self) -> Dict[str, Any]:
        """Perform Redis health check"""
        try:
            start_time = datetime.now()
            self.client.ping()
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            info = cast(Dict[str, Any], self.client.info())
            
            return {
                'status': 'healthy',
                'response_time_ms': response_time,
                'connected_clients': info.get('connected_clients', 0),
                'used_memory_human': info.get('used_memory_human', 'Unknown'),
                'uptime_in_seconds': info.get('uptime_in_seconds', 0)
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def close(self):
        """Close Redis connection"""
        try:
            self.client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

# Global Redis client instance
_redis_client: Optional[RedisClient] = None

def get_redis_client() -> RedisClient:
    """Get global Redis client instance"""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client

def close_redis_client():
    """Close global Redis client"""
    global _redis_client
    if _redis_client:
        _redis_client.close()
        _redis_client = None

# Convenience functions
def cache_set(key: str, value: Any, ttl: int = 3600) -> bool:
    """Set cache value with TTL"""
    return get_redis_client().set(key, value, ex=ttl)

def cache_get(key: str) -> Optional[str]:
    """Get cache value"""
    return get_redis_client().get(key)

def cache_delete(key: str) -> bool:
    """Delete cache key"""
    return bool(get_redis_client().delete(key))

# For testing
if __name__ == '__main__':
    print("Testing Redis client...")
    
    try:
        client = RedisClient()
        
        # Test basic operations
        client.set("test:key", "test_value", ex=60)
        value = client.get("test:key")
        print(f"Set/Get test: {value}")
        
        # Test JSON operations
        test_data = {"message": "Hello Redis", "timestamp": str(datetime.now())}
        client.set_json("test:json", test_data, ex=60)
        retrieved_data = client.get_json("test:json")
        print(f"JSON test: {retrieved_data}")
        
        # Health check
        health = client.health_check()
        print(f"Health check: {health}")
        
        # Cleanup
        client.delete("test:key", "test:json")
        client.close()
        
        print("✅ Redis client test completed successfully!")
        
    except Exception as e:
        print(f"❌ Redis client test failed: {e}")