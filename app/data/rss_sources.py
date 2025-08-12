"""
RSS Sources management with clean data separation
Imports from rss_sources_data.py for easy maintenance
"""

from typing import Dict, List, Any
from app.data.rss_sources_data import (
    RSS_SOURCES_DATA,
    RSSCategories,
    get_all_sources as _get_all_sources,
    get_sources_by_category as _get_sources_by_category,
    get_sources_by_region as _get_sources_by_region,
    get_high_reliability_sources as _get_high_reliability_sources,
    get_source_stats as _get_source_stats
)

# Re-export categories for easy access
class RSSSourceCategory:
    """RSS source categories for better organization"""
    GENERAL_NEWS = RSSCategories.GENERAL_NEWS
    TECHNOLOGY = RSSCategories.TECHNOLOGY
    BUSINESS = RSSCategories.BUSINESS
    FINANCE = RSSCategories.FINANCE  # Your new category
    POLITICS = RSSCategories.POLITICS
    SPORTS = RSSCategories.SPORTS
    ENTERTAINMENT = RSSCategories.ENTERTAINMENT
    SCIENCE = RSSCategories.SCIENCE
    HEALTH = RSSCategories.HEALTH
    STOCKS = RSSCategories.STOCKS        # Your new category
    STARTUPS = RSSCategories.STARTUPS    # Your new category
    AI = RSSCategories.AI               # Your new category

def get_all_sources() -> List[Dict[str, Any]]:
   
    sources = _get_all_sources()
    
   
    for source in sources:
       
        source["primary_region"] = source.pop("region", source.get("primary_region", "Global"))
        source["source_type"] = "rss"
        source["enabled"] = True
        if "country_code" not in source:
            source["country_code"] = _get_country_code(source["primary_region"])
        
        conflicting_fields = ["region", "category"] 
        for field in conflicting_fields:
            source.pop(field, None)
    
    return sources


def get_sources_by_region(region: str) -> List[Dict[str, Any]]:
    """Get sources for a specific region"""
    return _get_sources_by_region(region)

def get_sources_by_category(category: str) -> List[Dict[str, Any]]:
    """Get sources for a specific category across all regions"""
    return _get_sources_by_category(category)

def get_high_reliability_sources(min_score: int = 90) -> List[Dict[str, Any]]:
    """Get sources with reliability score above threshold"""
    return _get_high_reliability_sources(min_score)

def get_source_stats() -> Dict[str, Any]:
    """Get statistics about available sources"""
    return _get_source_stats()

def _get_country_code(region: str) -> str:
    """Map region to country code"""
    mapping = {
        "india": "IN",
        "us": "US", 
        "global": "GB",  # Default to GB for global
        "europe": "GB"
    }
    return mapping.get(region.lower(), "GB")

# Category-specific helper functions
def get_stocks_sources() -> List[Dict[str, Any]]:
    """Get all stocks-related RSS sources"""
    return get_sources_by_category(RSSCategories.STOCKS)

def get_startups_sources() -> List[Dict[str, Any]]:
    """Get all startups-related RSS sources"""
    return get_sources_by_category(RSSCategories.STARTUPS)

def get_ai_sources() -> List[Dict[str, Any]]:
    """Get all AI-related RSS sources"""
    return get_sources_by_category(RSSCategories.AI)

def get_india_sources() -> List[Dict[str, Any]]:
    """Get all India-specific sources"""
    return get_sources_by_region("India")

def get_tech_sources() -> List[Dict[str, Any]]:
    """Get all technology sources"""
    return get_sources_by_category(RSSCategories.TECHNOLOGY)

if __name__ == "__main__":
    # Print statistics
    stats = get_source_stats()
    print("RSS Sources Statistics:")
    print(f"Total Sources: {stats['total_sources']}")
    print(f"By Category: {stats['by_category']}")
    print(f"By Region: {stats['by_region']}")
    print(f"By Reliability: {stats['by_reliability']}")
    print(f"Average Poll Frequency: {stats['avg_poll_frequency']:.1f} minutes")
    
    # Show your new categories
    print(f"\n🔥 NEW CATEGORIES:")
    print(f"AI Sources: {len(get_ai_sources())}")
    print(f"Stocks Sources: {len(get_stocks_sources())}")
    print(f"Startups Sources: {len(get_startups_sources())}")
    
    # Show high reliability sources
    high_rel = get_high_reliability_sources(92)
    print(f"\nHigh Reliability Sources (92+): {len(high_rel)}")
    for source in high_rel[:5]:  # Show first 5
        print(f"- {source['name']} ({source['reliability_score']})")
