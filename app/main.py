from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, func
from typing import Optional, List
import os

# ✅ FIXED: Add missing imports
from app.database import AsyncSessionLocal
from app.models.article import Article
from app.models.source import NewsSource

# ENHANCED: Advanced caching integration
from app.services.cache_manager import get_cache_stats, cache_manager, get_cached_articles_smart
from app.tasks.rss_tasks import get_cache_performance_summary, warm_cache_layers

# Initialize FastAPI app
app = FastAPI(
    title="News Aggregator API",
    description="RSS News Aggregation with Advanced Multi-Layer Caching",
    version="2.0.0"
)

templates = Jinja2Templates(directory="templates")

# Mount static files directory for CSS/JS
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def news_dashboard(request: Request):
    """Main news dashboard page"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/articles")
async def get_articles(
    category: Optional[str] = None,
    search: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0
):
    """Get articles with optional filtering"""
    async with AsyncSessionLocal() as session:
        # ✅ FIXED: Use .desc() method instead of desc() function
        query = select(Article).order_by(Article.discovered_at.desc())
        
        # Apply filters
        if category and category != "all":
            query = query.filter(Article.primary_topic == category)
        
        if search:
            query = query.filter(
                Article.title.ilike(f"%{search}%") | 
                Article.content.ilike(f"%{search}%")
            )
        
        if source:
            query = query.filter(Article.source_name == source)
        
        # Apply pagination
        query = query.offset(offset).limit(limit)
        
        result = await session.execute(query)
        articles = result.scalars().all()
        
        # Convert to dict for JSON response
        articles_data = []
        for article in articles:
            articles_data.append({
                "id": article.id,
                "title": article.title,
                "content": (str(article.content or "")[:500] + "...") if len(str(article.content or "")) > 500 else str(article.content or ""),
                "summary": article.summary,
                "url": article.url,
                "source_name": article.source_name,
                "primary_topic": article.primary_topic,
                "secondary_topics": article.secondary_topics or [],
                "importance_level": article.importance_level,
                "primary_region": article.primary_region,
                "countries_mentioned": article.countries_mentioned or [],
                "quality_score": float(getattr(article, "quality_score", 0) or 0),
                "word_count": article.word_count,
                "reading_time_minutes": article.reading_time_minutes,
                "published_at": article.published_at.isoformat() if article.published_at is not None else None,
                "discovered_at": article.discovered_at.isoformat() if article.discovered_at is not None else None,
                "source_reliability": article.source_reliability
            })
        
        return {
            "articles": articles_data,
            "total": len(articles_data),
            "offset": offset,
            "limit": limit
        }


@app.get("/api/stats")
async def get_dashboard_stats():
    """Get dashboard statistics"""
    async with AsyncSessionLocal() as session:
        # Total articles
        total_articles = await session.scalar(select(func.count(Article.id)))
        
        # Articles by topic
        # ✅ FIXED: Use func.count().desc() instead of desc(func.count())
        topic_stats = await session.execute(
            select(Article.primary_topic, func.count(Article.id))
            .group_by(Article.primary_topic)
            .order_by(func.count(Article.id).desc())
        )
        topics = dict([tuple(row) for row in topic_stats.fetchall()])
        
        # Articles by source
        # ✅ FIXED: Use func.count().desc() instead of desc(func.count())
        source_stats = await session.execute(
            select(Article.source_name, func.count(Article.id))
            .group_by(Article.source_name)
            .order_by(func.count(Article.id).desc())
            .limit(10)
        )
        top_sources = dict([ (row[0], row[1]) for row in source_stats.fetchall() ])
        
        # Recent activity (last 24 hours)
        from datetime import datetime, timedelta
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_articles = await session.scalar(
            select(func.count(Article.id))
            .filter(Article.discovered_at >= yesterday)
        )
        
        return {
            "total_articles": total_articles,
            "topics": topics,
            "top_sources": top_sources,
            "recent_articles": recent_articles
        }


@app.get("/api/sources")
async def get_sources():
    """Get all news sources with their stats"""
    async with AsyncSessionLocal() as session:
        # ✅ FIXED: Use column.desc() instead of desc(column)
        result = await session.execute(
            select(NewsSource).order_by(NewsSource.reliability_score.desc()) # type: ignore[attr-defined]
        )
        sources = result.scalars().all()
        
        sources_data = []
        for source in sources:
            sources_data.append({
                "name": source.name,
                "url": source.url,
                "reliability_score": source.reliability_score,
                "total_articles_collected": source.total_articles_collected or 0,
                "successful_polls": source.successful_polls or 0,
                "failed_polls": source.failed_polls or 0,
                "primary_region": source.primary_region,
                "last_successful_poll_at": source.last_successful_poll_at.isoformat() if source.last_successful_poll_at is not None else None
            })
        
        return {"sources": sources_data}


# ENHANCED: Advanced Cache Management Endpoints for Priority 2

@app.get("/api/cache/stats")
async def get_cache_statistics():
    """Get comprehensive cache performance statistics"""
    try:
        cache_stats = get_cache_stats()
        performance_summary = get_cache_performance_summary()
        
        return {
            "cache_manager_stats": cache_stats,
            "performance_summary": performance_summary,
            "status": "healthy" if cache_stats else "error"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache statistics error: {str(e)}")


@app.post("/api/cache/warm")
async def warm_caches(layers: Optional[List[str]] = None):
    """Manually warm cache layers"""
    try:
        # Trigger cache warming task
        from app.tasks.rss_tasks import warm_cache_layers
        
        task_result = warm_cache_layers.delay(layers)
        
        return {
            "status": "warming_initiated",
            "task_id": task_result.id,
            "layers": layers or ["all"],
            "message": "Cache warming task started successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache warming error: {str(e)}")


@app.get("/api/cache/warm")
async def warm_caches_sync():
    """Synchronous cache warming (for immediate results)"""
    try:
        warming_stats = await cache_manager.warm_all_caches()
        
        return {
            "status": "completed",
            "warming_stats": warming_stats,
            "message": "All cache layers warmed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Synchronous cache warming error: {str(e)}")


@app.get("/api/articles/cached")
async def get_cached_articles(
    topic: Optional[str] = None,
    time_bucket: Optional[str] = Query(None, regex="^(1h|6h|24h)$"),
    limit: int = Query(50, le=200)
):
    """Get articles from advanced cache layers"""
    try:
        # Try to get articles from cache layers
        article_ids = await get_cached_articles_smart(topic, time_bucket, limit)
        
        if not article_ids:
            return {
                "articles": [],
                "source": "cache_miss",
                "cache_layer": "none",
                "total": 0
            }
        
        # Fetch full article data from database
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Article).filter(Article.id.in_(article_ids))
            )
            articles = result.scalars().all()
            
            articles_data = []
            for article in articles:
                articles_data.append({
                    "id": article.id,
                    "title": article.title,
                    "url": article.url,
                    "source_name": article.source_name,
                    "primary_topic": article.primary_topic,
                    "secondary_topics": article.secondary_topics or [],
                    "importance_level": article.importance_level,
                    "discovered_at": article.discovered_at.isoformat() if article.discovered_at is not None else None,
                    "reading_time_minutes": article.reading_time_minutes,
                    "source_reliability": article.source_reliability
                })
        
        return {
            "articles": articles_data,
            "source": "cache_hit",
            "cache_layer": "topic" if topic else "recency" if time_bucket else "smart",
            "total": len(articles_data),
            "requested_limit": limit
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cached articles retrieval error: {str(e)}")


@app.get("/api/cache/performance")
async def get_cache_performance():
    """Get detailed cache performance metrics"""
    try:
        analytics = cache_manager.get_cache_analytics()
        
        return {
            "manager_performance": analytics.get('manager_stats', {}),
            "redis_performance": analytics.get('redis_stats', {}),
            "cache_configuration": analytics.get('cache_config', {}),
            "recommendations": _get_cache_recommendations(analytics)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache performance error: {str(e)}")


@app.get("/api/cache/sources/top")
async def get_top_performing_sources_cached(limit: int = Query(10, le=50)):
    """Get top performing sources from cache"""
    try:
        top_sources = await cache_manager.get_top_performing_sources(limit)
        
        return {
            "top_sources": top_sources,
            "total": len(top_sources),
            "cached": True,
            "limit": limit
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Top sources cache error: {str(e)}")


@app.delete("/api/cache/invalidate/{topic}")
async def invalidate_topic_cache(topic: str):
    """Invalidate cache for a specific topic"""
    try:
        success = cache_manager.redis.invalidate_topic_cache(topic)
        
        if success:
            # Warm the cache again with fresh data
            warming_stats = await cache_manager.warm_topic_caches([topic])
            
            return {
                "status": "success",
                "topic": topic,
                "invalidated": True,
                "rewarmed": warming_stats.get(topic, 0) > 0
            }
        else:
            return {
                "status": "not_found",
                "topic": topic,
                "invalidated": False,
                "message": "Topic cache not found or already expired"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache invalidation error: {str(e)}")


@app.get("/api/cache/health")
async def get_cache_health():
    """Get cache system health status"""
    try:
        redis_health = cache_manager.redis.health_check()
        cache_analytics = cache_manager.get_cache_analytics()
        
        is_healthy = (
            redis_health.get('status') == 'healthy' and
            cache_analytics.get('manager_stats', {}).get('hit_ratio_percent', 0) > 50
        )
        
        return {
            "status": "healthy" if is_healthy else "degraded",
            "redis_health": redis_health,
            "cache_hit_ratio": cache_analytics.get('manager_stats', {}).get('hit_ratio_percent', 0),
            "total_operations": cache_analytics.get('manager_stats', {}).get('total_hits', 0) + 
                              cache_analytics.get('manager_stats', {}).get('total_misses', 0),
            "recommendations": _get_health_recommendations(redis_health, cache_analytics)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache health check error: {str(e)}")


# Helper functions
def _get_cache_recommendations(analytics: dict) -> List[str]:
    """Generate cache performance recommendations"""
    recommendations = []
    
    manager_stats = analytics.get('manager_stats', {})
    hit_ratio = manager_stats.get('hit_ratio_percent', 0)
    
    if hit_ratio < 70:
        recommendations.append("Consider increasing cache TTL values for better hit ratios")
    if hit_ratio < 50:
        recommendations.append("Cache warming may need to run more frequently")
    
    redis_stats = analytics.get('redis_stats', {})
    if redis_stats.get('total_keys', 0) > 10000:
        recommendations.append("Monitor memory usage - consider cache cleanup")
    
    return recommendations


def _get_health_recommendations(redis_health: dict, cache_analytics: dict) -> List[str]:
    """Generate cache health recommendations"""
    recommendations = []
    
    if redis_health.get('status') != 'healthy':
        recommendations.append("Redis connection issues detected - check Redis server")
    
    hit_ratio = cache_analytics.get('manager_stats', {}).get('hit_ratio_percent', 0)
    if hit_ratio < 60:
        recommendations.append("Low cache hit ratio - consider cache warming")
    
    return recommendations


# ENHANCED: RSS Task Management Endpoints
@app.get("/api/tasks/rss/trigger")
async def trigger_rss_collection():
    """Manually trigger RSS collection"""
    try:
        from app.tasks.rss_tasks import collect_all_rss_sources
        
        task_result = collect_all_rss_sources.delay()
        
        return {
            "status": "triggered",
            "task_id": task_result.id,
            "message": "RSS collection task started"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RSS collection trigger error: {str(e)}")


@app.get("/api/tasks/status/{task_id}")
async def get_task_status_endpoint(task_id: str):
    """Get status of a specific task"""
    try:
        from app.tasks.rss_tasks import get_task_status
        
        task_status = get_task_status(task_id)
        return task_status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Task status error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
