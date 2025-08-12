"""
RSS Sources Data - Easy to modify and maintain
Just add/remove entries from the list below to update sources
Supports: general_news, technology, business, politics, sports, entertainment, science, health, stocks, startups, ai
"""

from typing import List, Dict, Any

# RSS Source Categories
class RSSCategories:
    """RSS source categories for better organization"""
    GENERAL_NEWS = "general_news"
    TECHNOLOGY = "technology"
    BUSINESS = "business"
    FINANCE = "finance"  # New category for financial news
    POLITICS = "politics"
    SPORTS = "sports"
    ENTERTAINMENT = "entertainment"
    SCIENCE = "science"
    HEALTH = "health"
    STOCKS = "stocks"        # New category you added
    STARTUPS = "startups"    # New category you added
    AI = "ai"               # New category you added

# All RSS Sources - Simply add/remove entries to modify
RSS_SOURCES_DATA: List[Dict[str, Any]] = [
    
    # ========== INDIA - GENERAL NEWS ==========
    {
        "name": "The Hindu",
        "url": "https://www.thehindu.com/feeder/default.rss",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 95,
        "poll_frequency_minutes": 10,
        "topics": ["general", "politics", "india"],
        "category": RSSCategories.GENERAL_NEWS
    },
    {
        "name": "Indian Express",
        "url": "https://indianexpress.com/feed/",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 90,
        "poll_frequency_minutes": 10,
        "topics": ["general", "politics", "india"],
        "category": RSSCategories.GENERAL_NEWS
    },
    {
        "name": "Times of India",
        "url": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 85,
        "poll_frequency_minutes": 15,
        "topics": ["general", "india"],
        "category": RSSCategories.GENERAL_NEWS
    },
    {
        "name": "NDTV News",
        "url": "https://feeds.feedburner.com/NDTV-LatestNews",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 88,
        "poll_frequency_minutes": 12,
        "topics": ["general", "politics", "india"],
        "category": RSSCategories.GENERAL_NEWS
    },
    #this one is not for commercial use, so i should remember to remove it later
    {
        "name": "Hindustan Times",
        "url": "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 87,
        "poll_frequency_minutes": 15,
        "topics": ["general", "india"],
        "category": RSSCategories.GENERAL_NEWS
    },
    
    # ========== INDIA - BUSINESS ==========
    {
        "name": "Economic Times",
        "url": "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 92,
        "poll_frequency_minutes": 10,
        "topics": ["business", "economy", "india"],
        "category": RSSCategories.BUSINESS
    },
    {
        "name": "Business Standard",
        "url": "https://www.business-standard.com/rss/home_page_top_stories.rss",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 90,
        "poll_frequency_minutes": 12,
        "topics": ["business", "finance", "india"],
        "category": RSSCategories.BUSINESS
    },
    {
        "name": "Mint (Livemint)",
        "url": "https://www.livemint.com/rss/news",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 91,
        "poll_frequency_minutes": 10,
        "topics": ["business", "technology", "india"],
        "category": RSSCategories.BUSINESS
    },
    
    # ========== INDIA - TECHNOLOGY ==========
    {
        "name": "Inc42",
        "url": "https://inc42.com/feed/",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 88,
        "poll_frequency_minutes": 15,
        "topics": ["technology", "startups", "india"],
        "category": RSSCategories.TECHNOLOGY
    },
    {
        "name": "YourStory",
        "url": "https://yourstory.com/feed",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 85,
        "poll_frequency_minutes": 20,
        "topics": ["startups", "entrepreneurship", "india"],
        "category": RSSCategories.TECHNOLOGY
    },
    
    # ========== INDIA - STOCKS (NEW CATEGORY) ==========
    {
        "name": "Moneycontrol Markets",
        "url": "https://www.moneycontrol.com/rss/business.xml",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 89,
        "poll_frequency_minutes": 8,
        "topics": ["stocks", "markets", "india", "nse", "bse"],
        "category": RSSCategories.STOCKS
    },
    {
        "name": "Economic Times Markets",
        "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 90,
        "poll_frequency_minutes": 10,
        "topics": ["stocks", "equity", "india"],
        "category": RSSCategories.STOCKS
    },
    
    
    # ========== INDIA - STARTUPS (NEW CATEGORY) ==========
    {
        "name": "Entrepreneur India",
        "url": "https://www.entrepreneur.com/latest.rss",
        "region": "India",
        "country_code": "IN",
        "language": "en",
        "reliability_score": 86,
        "poll_frequency_minutes": 20,
        "topics": ["startups", "entrepreneurship", "india"],
        "category": RSSCategories.STARTUPS
    },
    # ========== GLOBAL - GENERAL NEWS ==========
    {
        "name": "BBC News",
        "url": "https://feeds.bbci.co.uk/news/rss.xml",
        "region": "Global",
        "country_code": "GB",
        "language": "en",
        "reliability_score": 95,
        "poll_frequency_minutes": 10,
        "topics": ["general", "international", "politics"],
        "category": RSSCategories.GENERAL_NEWS
    },
    {
        "name": "Reuters",
        "url": "https://ir.thomsonreuters.com/rss/news-releases.xml?items=15",
        "region": "Global",
        "country_code": "GB",
        "language": "en",
        "reliability_score": 96,
        "poll_frequency_minutes": 8,
        "topics": ["general", "business", "international"],
        "category": RSSCategories.GENERAL_NEWS
    },
    {
        "name": "CNN International",
        "url": "http://rss.cnn.com/rss/edition.rss",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 87,
        "poll_frequency_minutes": 15,
        "topics": ["general", "politics", "international"],
        "category": RSSCategories.GENERAL_NEWS
    },
    {
        "name": "The Guardian",
        "url": "https://www.theguardian.com/world/rss",
        "region": "Global",
        "country_code": "GB",
        "language": "en",
        "reliability_score": 92,
        "poll_frequency_minutes": 12,
        "topics": ["general", "politics", "international"],
        "category": RSSCategories.GENERAL_NEWS
    },
    
    # ========== GLOBAL - TECHNOLOGY ==========
    {
        "name": "TechCrunch",
        "url": "https://feeds.feedburner.com/TechCrunch",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 90,
        "poll_frequency_minutes": 15,
        "topics": ["technology", "startups", "ai"],
        "category": RSSCategories.TECHNOLOGY
    },
    {
        "name": "Ars Technica",
        "url": "https://feeds.arstechnica.com/arstechnica/index",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 92,
        "poll_frequency_minutes": 20,
        "topics": ["technology", "science"],
        "category": RSSCategories.TECHNOLOGY
    },
    {
        "name": "The Verge",
        "url": "https://www.theverge.com/rss/index.xml",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 89,
        "poll_frequency_minutes": 15,
        "topics": ["technology", "gadgets"],
        "category": RSSCategories.TECHNOLOGY
    },
    {
        "name": "Wired",
        "url": "https://www.wired.com/feed/rss",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 91,
        "poll_frequency_minutes": 20,
        "topics": ["technology", "science", "future"],
        "category": RSSCategories.TECHNOLOGY
    },
    
    # ========== GLOBAL - BUSINESS ==========
    {
        "name": "Wall Street Journal",
        "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 95,
        "poll_frequency_minutes": 10,
        "topics": ["business", "finance", "markets"],
        "category": RSSCategories.BUSINESS
    },
    {
        "name": "Financial Times",
        "url": "https://www.ft.com/rss/home",
        "region": "Global",
        "country_code": "GB",
        "language": "en",
        "reliability_score": 94,
        "poll_frequency_minutes": 12,
        "topics": ["business", "finance", "global"],
        "category": RSSCategories.BUSINESS
    },
    {
        "name": "Bloomberg",
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 93,
        "poll_frequency_minutes": 10,
        "topics": ["business", "markets", "finance"],
        "category": RSSCategories.BUSINESS
    },
    
    # ========== GLOBAL - STOCKS (NEW CATEGORY) ==========
    {
        "name": "MarketWatch",
        "url": "https://www.marketwatch.com/rss/topstories",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 90,
        "poll_frequency_minutes": 8,
        "topics": ["stocks", "markets", "trading"],
        "category": RSSCategories.STOCKS
    },
    {
        "name": "Yahoo Finance News",
        "url": "https://finance.yahoo.com/news/rssindex",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 87,
        "poll_frequency_minutes": 10,
        "topics": ["stocks", "finance", "markets"],
        "category": RSSCategories.STOCKS
    },
        {
        "name": "Yahoo Finance Stocks",
        "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=MSFT,AAPL&region=US&lang=en-US",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 87,
        "poll_frequency_minutes": 10,
        "topics": ["stocks", "finance", "markets"],
        "category": RSSCategories.FINANCE
    },
    {
        "name": "Seeking Alpha",
        "url": "https://seekingalpha.com/feed.xml",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 85,
        "poll_frequency_minutes": 15,
        "topics": ["stocks", "analysis", "investing"],
        "category": RSSCategories.STOCKS
    },
    
    # ========== GLOBAL - STARTUPS (NEW CATEGORY) ==========
    {
        "name": "Startup Grind",
        "url": "https://medium.com/feed/@StartupGrind",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 86,
        "poll_frequency_minutes": 20,
        "topics": ["startups", "entrepreneurship", "funding"],
        "category": RSSCategories.STARTUPS
    },
    {
        "name": "Crunchbase News",
        "url": "https://news.crunchbase.com/feed/",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 89,
        "poll_frequency_minutes": 18,
        "topics": ["startups", "venture", "funding"],
        "category": RSSCategories.STARTUPS
    },
    {
        "name": "Product Hunt",
        "url": "https://www.producthunt.com/feed",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 83,
        "poll_frequency_minutes": 30,
        "topics": ["startups", "products", "launches"],
        "category": RSSCategories.STARTUPS
    },
    {
        "name": "VentureBeat Startups",
        "url": "https://venturebeat.com/category/startup/feed/",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 87,
        "poll_frequency_minutes": 25,
        "topics": ["startups", "technology", "venture"],
        "category": RSSCategories.STARTUPS
    },
    
    # ========== GLOBAL - AI (NEW CATEGORY) ==========
    {
        "name": "AI News",
        "url": "https://www.artificialintelligence-news.com/feed/",
        "region": "Global",
        "country_code": "GB",
        "language": "en",
        "reliability_score": 91,
        "poll_frequency_minutes": 12,
        "topics": ["ai", "machine learning", "technology"],
        "category": RSSCategories.AI
    },
    {
        "name": "MIT AI News",
        "url": "https://news.mit.edu/rss/topic/artificial-intelligence2",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 95,
        "poll_frequency_minutes": 15,
        "topics": ["ai", "research", "machine learning"],
        "category": RSSCategories.AI
    },
    {
        "name": "OpenAI Blog",
        "url": "https://openai.com/blog/rss.xml",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 93,
        "poll_frequency_minutes": 20,
        "topics": ["ai", "gpt", "openai"],
        "category": RSSCategories.AI
    }, 
    {
        "name": "Google Tech Blog",
        "url": "https://blog.google/technology/rss",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 94,
        "poll_frequency_minutes": 18,
        "topics": ["technology", "google", "research"],
        "category": RSSCategories.TECHNOLOGY
    },
    {
        "name": "Google AI Blog",
        "url": "https://blog.google/technology/ai/rss/",
        "region": "Global",
        "country_code": "US",
        "language": "en",
        "reliability_score": 94,
        "poll_frequency_minutes": 18,
        "topics": ["ai", "google", "research"],
        "category": RSSCategories.AI
    },
    # {
    #     "name": "The Batch (deeplearning.ai)",
    #     "url": "https://www.deeplearning.ai/the-batch/rss.xml",
    #     "region": "Global",
    #     "country_code": "US",
    #     "language": "en",
    #     "reliability_score": 92,
    #     "poll_frequency_minutes": 25,
    #     "topics": ["ai", "deep learning", "andrew ng"],
    #     "category": RSSCategories.AI
    # },

    
    # ========== US - SPECIFIC ==========
    {
        "name": "New York Times",
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
        "region": "US",
        "country_code": "US",
        "language": "en",
        "reliability_score": 94,
        "poll_frequency_minutes": 12,
        "topics": ["general", "politics", "us"],
        "category": RSSCategories.GENERAL_NEWS
    },

    {
        "name": "Mashable All",
        "url": "https://mashable.com/feeds/rss/all",
        "region": "US",
        "country_code": "US",
        "language": "en",
        "reliability_score": 84,
        "poll_frequency_minutes": 20,
        "topics": ["technology", "social_media"],
        "category": RSSCategories.GENERAL_NEWS
    },
     {
        "name": "Mashable Tech",
        "url": "https://mashable.com/feeds/rss/tech",
        "region": "US",
        "country_code": "US",
        "language": "en",
        "reliability_score": 84,
        "poll_frequency_minutes": 20,
        "topics": ["general", "social_media"],
        "category": RSSCategories.TECHNOLOGY
    },
    
    # ========== ADD MORE SOURCES EASILY ==========
    # Just copy the format above and add new entries here
    # Example:
    # {
    #     "name": "Source Name",
    #     "url": "https://example.com/feed.rss",
    #     "region": "Region",
    #     "country_code": "XX",
    #     "language": "en",
    #     "reliability_score": 90,
    #     "poll_frequency_minutes": 15,
    #     "topics": ["topic1", "topic2"],
    #     "category": RSSCategories.CATEGORY_NAME
    # },
]

# Quick access functions
def get_all_sources() -> List[Dict[str, Any]]:
    """Get all RSS sources"""
    return [source.copy() for source in RSS_SOURCES_DATA]

def get_sources_by_category(category: str) -> List[Dict[str, Any]]:
    """Get sources by category"""
    return [source.copy() for source in RSS_SOURCES_DATA if source["category"] == category]

def get_sources_by_region(region: str) -> List[Dict[str, Any]]:
    """Get sources by region"""
    return [source.copy() for source in RSS_SOURCES_DATA if source["region"].lower() == region.lower()]

def get_high_reliability_sources(min_score: int = 90) -> List[Dict[str, Any]]:
    """Get high reliability sources"""
    return [source.copy() for source in RSS_SOURCES_DATA if source["reliability_score"] >= min_score]

# Statistics
def get_source_stats() -> Dict[str, Any]:
    """Get statistics about RSS sources"""
    all_sources = RSS_SOURCES_DATA
    
    # Count by category
    categories = {}
    for source in all_sources:
        cat = source["category"]
        categories[cat] = categories.get(cat, 0) + 1
    
    # Count by region
    regions = {}
    for source in all_sources:
        reg = source["region"]
        regions[reg] = regions.get(reg, 0) + 1
    
    # Reliability breakdown
    excellent = len([s for s in all_sources if s["reliability_score"] >= 95])
    very_good = len([s for s in all_sources if 90 <= s["reliability_score"] < 95])
    good = len([s for s in all_sources if 85 <= s["reliability_score"] < 90])
    average = len([s for s in all_sources if s["reliability_score"] < 85])
    
    return {
        "total_sources": len(all_sources),
        "by_category": categories,
        "by_region": regions,
        "by_reliability": {
            "excellent": excellent,
            "very_good": very_good,
            "good": good,
            "average": average
        },
        "avg_poll_frequency": sum(s["poll_frequency_minutes"] for s in all_sources) / len(all_sources)
    }

if __name__ == "__main__":
    # Print statistics
    stats = get_source_stats()
    print("RSS Sources Statistics:")
    print(f"Total Sources: {stats['total_sources']}")
    print(f"By Category: {stats['by_category']}")
    print(f"By Region: {stats['by_region']}")
    print(f"By Reliability: {stats['by_reliability']}")
    print(f"Average Poll Frequency: {stats['avg_poll_frequency']:.1f} minutes")
    
    # Show AI sources
    ai_sources = get_sources_by_category(RSSCategories.AI)
    print(f"\nAI Sources ({len(ai_sources)}):")
    for source in ai_sources:
        print(f"- {source['name']} (Score: {source['reliability_score']})")
