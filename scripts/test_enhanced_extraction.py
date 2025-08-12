#!/usr/bin/env python3
"""Test enhanced content extraction"""

import asyncio
import aiohttp
import feedparser
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.rss_collector import RSSCollector

async def test_content_extraction():
    """Test the enhanced content extraction on specific feeds"""
    
    test_feeds = [
        ("BBC News", "https://feeds.bbci.co.uk/news/rss.xml"),
        ("YourStory", "https://yourstory.com/feed"),
        ("Inc42", "https://inc42.com/feed/"),
        ("Economic Times", "https://economictimes.indiatimes.com/rssfeedsdefault.cms"),
    ]
    
    async with RSSCollector() as collector:
        for name, url in test_feeds:
            print(f"\n🔍 Testing: {name}")
            print(f"URL: {url}")
            
            try:
                # Import NewsSource and create source object
                from app.models.source import NewsSource

                # Fetch feed
                feed_data = await collector._fetch_rss_with_retry(NewsSource(name=name, url=url))
                
                if feed_data:
                    feed = feedparser.parse(feed_data)
                    print(f"📰 Feed entries found: {len(feed.entries)}")
                    
                    if feed.entries:
                        entry = feed.entries[0]
                        
                        # Test enhanced extraction
                        content = collector._extract_entry_content(entry)
                        
                        print(f"📄 Content extracted: {len(content)} characters")
                        if content:
                            print(f"📝 Content preview: {content[:200]}...")
                            
                            # Show content quality
                            if len(content) > 500:
                                print("✅ Good content length")
                            elif len(content) > 100:
                                print("⚠️  Moderate content length")
                            else:
                                print("❌ Short content")
                        else:
                            print("❌ No content extracted")
                            
                            # Debug: show what fields are available
                            print("🔍 Available fields:")
                            for attr in dir(entry):
                                if not attr.startswith('_') and hasattr(entry, attr):
                                    value = getattr(entry, attr)
                                    if value and len(str(value)) > 10:
                                        print(f"   {attr}: {str(value)[:100]}...")
                
                else:
                    print("❌ Failed to fetch feed")
                    
            except Exception as e:
                print(f"❌ Error testing {name}: {e}")

if __name__ == "__main__":
    asyncio.run(test_content_extraction())
