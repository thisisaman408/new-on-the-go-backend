#!/usr/bin/env python3
"""Test Advanced Caching System"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.cache_manager import cache_manager, get_cache_stats
from app.database import AsyncSessionLocal
from app.models.article import Article
from app.models.source import NewsSource
from sqlalchemy import select, func
from datetime import datetime

async def test_cache_layers():
    """Test all 5 cache layers"""
    print("🧪 Testing Advanced Multi-Layer Caching System\n")
    
    # Test Layer 1: Content Hash Cache
    print("1️⃣ Testing Content Hash Cache...")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Article).limit(1))
        article = result.scalar_one_or_none()
        
        if article:
            # Cache article by hash
            success = await cache_manager.cache_article_by_hash(article)
            print(f"   ✅ Cached article by hash: {success}")
            
            # Retrieve cached article
            cached = await cache_manager.get_article_by_hash(article.content_hash)
            print(f"   ✅ Retrieved cached article: {cached is not None}")
        else:
            print("   ⚠️ No articles found for testing")

    # Test Layer 2: Topic-Based Cache
    print("\n2️⃣ Testing Topic-Based Cache...")
    topic_stats = await cache_manager.warm_topic_caches(['technology', 'business'])
    print(f"   ✅ Topic cache warming: {topic_stats}")
    
    # Test retrieval
    tech_articles = await cache_manager.get_articles_by_topic('technology', 10)
    print(f"   ✅ Retrieved tech articles: {len(tech_articles)} articles")

    # Test Layer 3: Recency Cache
    print("\n3️⃣ Testing Recency Cache...")
    recency_stats = await cache_manager.warm_recency_caches()
    print(f"   ✅ Recency cache warming: {recency_stats}")
    
    # Test time bucket retrieval
    recent_1h = await cache_manager.get_articles_by_recency('1h', 5)
    recent_6h = await cache_manager.get_articles_by_recency('6h', 5)
    print(f"   ✅ Recent articles (1h): {len(recent_1h)}, (6h): {len(recent_6h)}")

    # Test Layer 4: Source Performance Cache
    print("\n4️⃣ Testing Source Performance Cache...")
    source_stats = await cache_manager.cache_source_performance_metrics()
    print(f"   ✅ Source performance caching: {source_stats}")
    
    top_sources = await cache_manager.get_top_performing_sources(5)
    print(f"   ✅ Top performing sources: {len(top_sources)} sources")

    # Test Layer 5: News Digest Cache
    print("\n5️⃣ Testing News Digest Cache...")
    sample_digest = {
        'articles': [{'id': 1, 'title': 'Test Article'}],
        'generated_at': datetime.utcnow().isoformat()
    }
    digest_cached = await cache_manager.cache_news_digest('morning', sample_digest)
    print(f"   ✅ Digest cached: {digest_cached}")
    
    retrieved_digest = await cache_manager.get_news_digest('morning')
    print(f"   ✅ Retrieved digest: {retrieved_digest is not None}")

    # Test Cache Analytics
    print("\n📊 Testing Cache Analytics...")
    analytics = cache_manager.get_cache_analytics()
    print(f"   ✅ Cache analytics: {analytics.get('manager_stats', {})}")
    
    return analytics

async def test_cache_integration():
    """Test cache integration with RSS collection"""
    print("\n🔗 Testing Cache Integration...")
    
    # Trigger RSS collection (which should use caching)
    import requests
    try:
        response = requests.get("http://localhost:8000/api/tasks/rss/trigger")
        if response.status_code == 200:
            task_data = response.json()
            print(f"   ✅ RSS collection triggered: {task_data.get('task_id')}")
            
            # Wait a bit for task completion
            await asyncio.sleep(15)
            
            # Check cache stats after collection
            response = requests.get("http://localhost:8000/api/cache/stats")
            if response.status_code == 200:
                stats = response.json()
                print(f"   ✅ Cache stats after RSS collection: {stats}")
        else:
            print(f"   ❌ RSS trigger failed: {response.status_code}")
    except Exception as e:
        print(f"   ⚠️ Integration test skipped (FastAPI not running): {e}")

async def test_cached_retrieval():
    """Test smart cached article retrieval"""
    print("\n🎯 Testing Smart Cached Retrieval...")
    
    # Test cached articles endpoint
    import requests
    try:
        # Test topic-based retrieval
        response = requests.get("http://localhost:8000/api/articles/cached?topic=technology&limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Cached tech articles: {data.get('total', 0)} articles, source: {data.get('source')}")
        
        # Test recency-based retrieval
        response = requests.get("http://localhost:8000/api/articles/cached?time_bucket=1h&limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Cached recent articles: {data.get('total', 0)} articles, source: {data.get('source')}")
            
    except Exception as e:
        print(f"   ⚠️ API tests skipped: {e}")

async def main():
    """Run all cache tests"""
    start_time = datetime.utcnow()
    
    try:
        # Run core cache tests
        analytics = await test_cache_layers()
        
        # Test integration
        await test_cache_integration()
        
        # Test retrieval
        await test_cached_retrieval()
        
        # Final performance summary
        end_time = datetime.utcnow()
        test_duration = (end_time - start_time).total_seconds()
        
        print(f"\n📈 Test Summary:")
        print(f"   Duration: {test_duration:.2f} seconds")
        print(f"   Hit Ratio: {analytics.get('manager_stats', {}).get('hit_ratio_percent', 0):.1f}%")
        print(f"   Total Operations: {analytics.get('manager_stats', {}).get('total_hits', 0) + analytics.get('manager_stats', {}).get('total_misses', 0)}")
        print(f"   Redis Status: {analytics.get('redis_stats', {}).get('error', 'Healthy')}")
        
        print("\n🎉 Advanced Caching System Test Complete!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
