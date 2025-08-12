#!/usr/bin/env python3
"""Seed database with comprehensive RSS sources - FIXED VERSION"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Set
from sqlalchemy import select, func, delete

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import AsyncSessionLocal
from app.models.source import NewsSource
from app.data.rss_sources import get_all_sources, get_source_stats
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def cleanup_true_duplicates():
    """Clean up only TRUE duplicates (same name AND same URL)"""
    async with AsyncSessionLocal() as session:
        try:
            logger.info("🧹 Cleaning up TRUE duplicate sources...")
            
            # Find duplicates by both name AND URL (not just URL)
            result = await session.execute(
                select(NewsSource.name, NewsSource.url, func.count(NewsSource.id).label('count'))
                .group_by(NewsSource.name, NewsSource.url)
                .having(func.count(NewsSource.id) > 1)
            )
            
            duplicate_pairs = [(row.name, row.url) for row in result.fetchall()]
            removed_count = 0
            
            for name, url in duplicate_pairs:
                # Get all sources with this exact name AND URL combination
                result = await session.execute(
                    select(NewsSource)
                    .filter(NewsSource.name == name, NewsSource.url == url)
                    .order_by(NewsSource.created_at.desc())  # Keep the newest
                )
                
                sources = list(result.scalars().all())
                if len(sources) > 1:
                    # Remove all but the first (newest)
                    for source in sources[1:]:
                        await session.delete(source)
                        removed_count += 1
                        logger.info(f"   Removed TRUE duplicate: {source.name} ({source.id})")
            
            await session.commit()
            logger.info(f"✅ Removed {removed_count} TRUE duplicate sources")
            
        except Exception as e:
            await session.rollback()
            logger.error(f"❌ Cleanup failed: {e}")
            raise

async def create_unique_source_key(source_data: Dict) -> str:
    """Create unique key for source identification"""
    # Use name + URL + category to differentiate legitimate multiple feeds
    return f"{source_data['name']}||{source_data['url']}||{source_data.get('category', 'unknown')}"

async def seed_sources():
    """Add comprehensive RSS sources with proper duplicate handling"""
    async with AsyncSessionLocal() as session:
        try:
            # Get all sources from our curated list
            all_sources = get_all_sources()
            logger.info(f"Found {len(all_sources)} RSS sources to seed")
            
            # Fix the Entrepreneur India URL issue
            for source in all_sources:
                if (source.get('name') == 'Entrepreneur India' and 
                    source.get('url') == 'https://www.entrepreneur.com/latest.rss'):
                    source['url'] = 'https://www.entrepreneur.com/india/feed'
                    logger.info("🔧 Fixed Entrepreneur India URL")
            
            # Bulk load existing sources
            result = await session.execute(select(NewsSource))
            existing_sources = {}
            for source in result.scalars().all():
                key = f"{source.name}||{source.url}||{getattr(source, 'category', 'unknown')}"
                existing_sources[key] = source
            
            logger.info(f"Found {len(existing_sources)} existing sources in database")
            
            # Create unique sources map with enhanced key
            unique_sources = {}
            duplicate_info = []
            
            for source_data in all_sources:
                key = await create_unique_source_key(source_data)
                
                if key in unique_sources:
                    duplicate_info.append({
                        'name': source_data['name'],
                        'url': source_data['url'],
                        'category': source_data.get('category', 'unknown')
                    })
                    logger.warning(f"⚠️  True duplicate found in data: {source_data['name']} - {source_data['url']}")
                    continue
                    
                unique_sources[key] = source_data
            
            logger.info(f"After deduplication: {len(unique_sources)} unique sources to process")
            if duplicate_info:
                logger.info(f"Found {len(duplicate_info)} true duplicates in source data")
            
            added_count = 0
            updated_count = 0
            skipped_count = 0
            
            for key, source_data in unique_sources.items():
                try:
                    existing_source = existing_sources.get(key)
                    
                    if existing_source:
                        # Update existing source
                        updated = False
                        for field, value in source_data.items():
                            if hasattr(existing_source, field) and getattr(existing_source, field) != value:
                                setattr(existing_source, field, value)
                                updated = True
                        
                        # Always update next poll time
                        existing_source.next_poll_at = datetime.utcnow() + timedelta(
                            minutes=source_data.get('poll_frequency_minutes', 15)
                        )
                        
                        if updated:
                            updated_count += 1
                            logger.info(f"Updated source: {source_data['name']} ({source_data.get('category', '')})")
                        else:
                            skipped_count += 1
                    else:
                        # Create new source
                        source = NewsSource(**source_data)
                        session.add(source)
                        added_count += 1
                        logger.info(f"Added new source: {source_data['name']} ({source_data.get('category', '')})")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing source {source_data.get('name', 'Unknown')}: {e}")
                    continue
            
            await session.commit()
            
            # Print summary with duplicate handling info
            logger.info("✅ Database seeding completed successfully!")
            logger.info(f"📊 Summary:")
            logger.info(f"   • New sources added: {added_count}")
            logger.info(f"   • Existing sources updated: {updated_count}")
            logger.info(f"   • Sources skipped (no changes): {skipped_count}")
            logger.info(f"   • Total unique sources processed: {len(unique_sources)}")
            
            # Show legitimate multiple feeds
            legitimate_multiples = await get_legitimate_multiple_feeds()
            if legitimate_multiples:
                logger.info(f"   • Legitimate multiple feeds detected:")
                for group in legitimate_multiples:
                    logger.info(f"     - {group['name']}: {group['count']} feeds ({', '.join(group['types'])})")
            
            return {
                'added': added_count,
                'updated': updated_count,
                'skipped': skipped_count,
                'total_processed': len(unique_sources),
                'legitimate_multiples': len(legitimate_multiples)
            }
            
        except Exception as e:
            await session.rollback()
            logger.error(f"❌ Seeding failed: {e}")
            raise

async def get_legitimate_multiple_feeds():
    """Identify sources with multiple legitimate feeds - FIXED VERSION"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(NewsSource.name, func.count(NewsSource.id).label('count'))
            .group_by(NewsSource.name)
            .having(func.count(NewsSource.id) > 1)
        )
        
        multiple_feeds = []
        for row in result.fetchall():
            # 🔧 FIX: Get full NewsSource objects, not just columns
            sources_result = await session.execute(
                select(NewsSource)  # ← Changed from select(NewsSource.url, NewsSource.topics)
                .filter(NewsSource.name == row.name)
            )
            
            sources = list(sources_result.scalars().all())  # ← Now these are NewsSource objects
            feed_types = []
            
            for source in sources:
                # 🔧 FIX: Now we can safely access .topics because source is a NewsSource object
                if source.topics is not None and isinstance(source.topics, list):
                    feed_types.extend(source.topics[:2])  # First 2 topics
                    
            multiple_feeds.append({
                'name': row.name,
                'count': row.count,
                'types': list(set(feed_types))[:3]  # Unique types, max 3
            })
            
        return multiple_feeds

async def enhanced_rss_content_extraction():
    """Diagnose and fix content extraction issues"""
    logger.info("🔍 Diagnosing RSS content extraction issues...")
    
    # Test problematic feeds
    test_feeds = [
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://inc42.com/feed/",
        "https://yourstory.com/feed",
        "https://www.entrepreneur.com/latest.rss"
    ]
    
    import aiohttp
    import feedparser
    
    for feed_url in test_feeds:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(feed_url) as response:
                    if response.status == 200:
                        content = await response.text()
                        feed = feedparser.parse(content)
                        
                        if feed.entries:
                            entry = feed.entries[0]
                            
                            # Check different content fields
                            content_fields = {
                                'content': getattr(entry, 'content', None),
                                'description': getattr(entry, 'description', None),
                                'summary': getattr(entry, 'summary', None),
                                'subtitle': getattr(entry, 'subtitle', None)
                            }
                            
                            logger.info(f"📰 Feed: {feed_url}")
                            for field, value in content_fields.items():
                                if value:
                                    if isinstance(value, list):
                                        logger.info(f"   {field}: List with {len(value)} items")
                                        if value:
                                            logger.info(f"   {field}[0]: {str(value[0])[:100]}...")
                                    else:
                                        logger.info(f"   {field}: {str(value)[:100]}...")
                                else:
                                    logger.info(f"   {field}: None")
                            logger.info("")
        except Exception as e:
            logger.error(f"❌ Error testing {feed_url}: {e}")

async def verify_seeded_data():
    """Enhanced verification with duplicate analysis"""
    async with AsyncSessionLocal() as session:
        try:
            # Count total sources
            result = await session.execute(select(func.count(NewsSource.id)))
            total_count = result.scalar()
            
            # Count by region
            result = await session.execute(
                select(NewsSource.primary_region, func.count(NewsSource.id))
                .group_by(NewsSource.primary_region)
            )
            region_counts = dict(result.fetchall())
            
            # Count enabled sources
            result = await session.execute(
                select(func.count(NewsSource.id))
                .where(NewsSource.enabled.is_(True))
            )
            enabled_count = result.scalar()
            
            # Check for TRUE duplicates (same name + same URL)
            result = await session.execute(
                select(NewsSource.name, NewsSource.url, func.count(NewsSource.id).label('count'))
                .group_by(NewsSource.name, NewsSource.url)
                .having(func.count(NewsSource.id) > 1)
            )
            true_duplicates = result.fetchall()
            
            # Check legitimate multiple feeds
            legitimate_multiples = await get_legitimate_multiple_feeds()
            
            logger.info("🔍 Database Verification:")
            logger.info(f"   • Total sources in database: {total_count}")
            logger.info(f"   • Enabled sources: {enabled_count}")
            logger.info(f"   • Sources by region: {region_counts}")
            logger.info(f"   • TRUE duplicates found: {len(true_duplicates)}")
            logger.info(f"   • Legitimate multiple feeds: {len(legitimate_multiples)}")
            
            if true_duplicates:
                logger.warning("⚠️  TRUE duplicates still exist:")
                for row in true_duplicates:
                    logger.warning(f"     - {row.name}: {row.url} ({row.count} entries)")
            
            if legitimate_multiples:
                logger.info("✅ Legitimate multiple feeds:")
                for group in legitimate_multiples:
                    logger.info(f"     - {group['name']}: {group['count']} feeds")
            
            return {
                'total_count': total_count,
                'enabled_count': enabled_count,
                'region_counts': region_counts,
                'true_duplicate_count': len(true_duplicates),
                'legitimate_multiple_count': len(legitimate_multiples)
            }
                
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return {}

async def main():
    """Main function with enhanced diagnostics"""
    try:
        print("🌱 Starting RSS sources seeding with enhanced duplicate handling...")
        
        # Step 1: Clean up TRUE duplicates only
        await cleanup_true_duplicates()
        
        # Step 2: Diagnose content extraction issues
        await enhanced_rss_content_extraction()
        
        # Step 3: Seed sources with proper duplicate handling
        seed_results = await seed_sources()
        
        # Step 4: Verify results
        print("\n🔍 Verifying seeded data...")
        verify_results = await verify_seeded_data()
        
        # Final summary
        print(f"\n✅ Seeding complete!")
        print(f"📊 Final Results:")
        print(f"   • Total sources in database: {verify_results.get('total_count', 'Unknown')}")
        print(f"   • New sources added: {seed_results.get('added', 0)}")
        print(f"   • Sources updated: {seed_results.get('updated', 0)}")
        print(f"   • TRUE duplicates remaining: {verify_results.get('true_duplicate_count', 0)}")
        print(f"   • Legitimate multiple feeds: {verify_results.get('legitimate_multiple_count', 0)}")
        
        if verify_results.get('true_duplicate_count', 0) == 0:
            print("🎉 No TRUE duplicates found! Database is clean.")
            print("✅ Multiple feeds for same sources are preserved as intended.")
        
        print("\n🚀 Ready to start RSS collection with enhanced content extraction!")
        
    except Exception as e:
        logger.error(f"❌ Main process failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
