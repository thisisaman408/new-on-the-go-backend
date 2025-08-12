#!/usr/bin/env python3
"""Debug individual RSS feeds to identify issues"""

import asyncio
import aiohttp
import feedparser
import sys
import logging
from datetime import datetime

# Add the parent directory to the path
sys.path.append('..')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_individual_feed(url, name):
    """Test a single RSS feed and provide detailed diagnostics"""
    print(f"\n🔍 Testing: {name}")
    print(f"URL: {url}")
    
    try:
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; NewsAggregator/1.0)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url) as response:
                print(f"📡 HTTP Status: {response.status}")
                print(f"📄 Content-Type: {response.headers.get('content-type', 'Unknown')}")
                print(f"📏 Content-Length: {response.headers.get('content-length', 'Unknown')}")
                
                if response.status != 200:
                    print(f"❌ HTTP Error: {response.status}")
                    # Try to get error content
                    try:
                        error_content = await response.text()
                        print(f"🔍 Error Response Preview: {error_content[:200]}...")
                    except:
                        pass
                    return False
                
                content = await response.text()
                print(f"📊 Content Size: {len(content)} characters")
                
                # Show raw content preview for debugging
                print(f"📄 Raw content preview (first 300 chars):")
                print(content[:300])
                print("..." if len(content) > 300 else "")
                
                # Parse with feedparser
                feed = feedparser.parse(content)
                
                print(f"📰 Feed Title: {feed.feed.get('title', 'No title')}")  #type: ignore[assignment]
                print(f"📝 Feed Description: {feed.feed.get('description', 'No description')[:100]}...")  #type: ignore[assignment]
                print(f"📰 Total Entries: {len(feed.entries)}")
                
                if len(feed.entries) == 0:
                    print("❌ No entries found in feed!")
                    print("🔍 Checking for common issues...")
                    
                    # Check if it's actually HTML instead of XML
                    if content.lower().startswith('<!doctype') or '<html' in content.lower():
                        print("⚠️  Response is HTML, not XML RSS feed")
                    
                    # Check if it's a valid XML structure
                    if '<?xml' not in content:
                        print("⚠️  No XML declaration found")
                    
                    # Check for RSS/feed tags
                    if '<rss' not in content.lower() and '<feed' not in content.lower():
                        print("⚠️  No RSS or Atom feed tags found")
                    
                    return False
                
                # Show first few entries
                print(f"📋 Sample entries:")
                for i, entry in enumerate(feed.entries[:3]):
                    title = entry.get('title')
                    if title is None:
                        title = 'No title'
                    print(f"  {i+1}. {title[:80]}...")
                    print(f"     Published: {entry.get('published', 'No date')}")
                    print(f"     Link: {entry.get('link', 'No link')}")
                
                print("✅ Feed is working correctly!")
                return True
                
    except asyncio.TimeoutError:
        print("❌ Timeout - Feed took too long to respond")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

async def main():
    # Test YOUR EXACT problematic feeds from database query
    problem_feeds = [
        ("Reuters", "https://ir.thomsonreuters.com/rss/news-releases.xml?items=15"),
        ("Mint (Livemint)", "https://www.livemint.com/rss/news"),
        ("Business Standard", "https://www.business-standard.com/rss/home_page_top_stories.rss"),
        ("Moneycontrol Markets", "https://www.moneycontrol.com/rss/business.xml"),
        ("VentureBeat Startups", "https://venturebeat.com/category/startup/feed/"),
        ("Entrepreneur India", "https://www.entrepreneur.com/latest.rss"),  # From your data file
        ("Hindustan Times", "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml"),  # From your data file
    ]
    
    print("🚀 Starting RSS Feed Diagnostics with YOUR EXACT URLs...\n")
    
    working_feeds = []
    broken_feeds = []
    
    for name, url in problem_feeds:
        result = await test_individual_feed(url, name)
        if result:
            working_feeds.append((name, url))
        else:
            broken_feeds.append((name, url))
        
        await asyncio.sleep(2)  # Be respectful to servers
    
    print(f"\n📊 Summary:")
    print(f"✅ Working feeds: {len(working_feeds)}")
    print(f"❌ Broken feeds: {len(broken_feeds)}")
    
    if working_feeds:
        print(f"\n✅ Working feeds:")
        for name, url in working_feeds:
            print(f"  - {name}")
    
    if broken_feeds:
        print(f"\n🔧 Feeds needing fixes:")
        for name, url in broken_feeds:
            print(f"  - {name}: {url}")

if __name__ == "__main__":
    asyncio.run(main())
