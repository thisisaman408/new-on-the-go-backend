"""
Countries mapping for geographic detection in articles
Maps country names to common aliases and variations
"""

from typing import Dict, List

# Comprehensive mapping of countries to their aliases/variations
COUNTRIES_MAP: Dict[str, List[str]] = {
    # Major English-speaking countries
    "United States": [
        "usa", "us", "united states", "america", "u.s.a", "u.s.", 
        "american", "states", "washington", "new york", "california"
    ],
    
    "United Kingdom": [
        "uk", "britain", "great britain", "england", "scotland", "wales", 
        "british", "london", "u.k.", "united kingdom"
    ],
    
    "Canada": [
        "canada", "canadian", "toronto", "vancouver", "montreal", "ottawa"
    ],
    
    "Australia": [
        "australia", "australian", "sydney", "melbourne", "canberra", "aussie"
    ],
    
    # India (primary focus)
    "India": [
        "india", "indian", "bharat", "hindustan", "delhi", "mumbai", 
        "bangalore", "chennai", "kolkata", "hyderabad", "pune", "new delhi"
    ],
    
    # Major Asian countries
    "China": [
        "china", "chinese", "beijing", "shanghai", "hong kong", "prc"
    ],
    
    "Japan": [
        "japan", "japanese", "tokyo", "osaka", "nippon"
    ],
    
    "South Korea": [
        "south korea", "korea", "korean", "seoul", "rok"
    ],
    
    "Singapore": [
        "singapore", "singaporean"
    ],
    
    # Major European countries
    "Germany": [
        "germany", "german", "deutschland", "berlin", "munich"
    ],
    
    "France": [
        "france", "french", "paris", "lyon"
    ],
    
    "Russia": [
        "russia", "russian", "moscow", "kremlin"
    ],
    
    "Italy": [
        "italy", "italian", "rome", "milan"
    ],
    
    "Spain": [
        "spain", "spanish", "madrid", "barcelona"
    ],
    
    "Netherlands": [
        "netherlands", "dutch", "holland", "amsterdam"
    ],
    
    "Switzerland": [
        "switzerland", "swiss", "zurich", "geneva"
    ],
    
    # Middle East
    "Israel": [
        "israel", "israeli", "jerusalem", "tel aviv"
    ],
    
    "Saudi Arabia": [
        "saudi arabia", "saudi", "riyadh"
    ],
    
    "UAE": [
        "uae", "emirates", "dubai", "abu dhabi", "united arab emirates"
    ],
    
    # Other important countries
    "Brazil": [
        "brazil", "brazilian", "sao paulo", "rio de janeiro"
    ],
    
    "Mexico": [
        "mexico", "mexican", "mexico city"
    ],
    
    "Argentina": [
        "argentina", "argentinian", "buenos aires"
    ],
    
    "South Africa": [
        "south africa", "south african", "cape town", "johannesburg"
    ],
    
    "Nigeria": [
        "nigeria", "nigerian", "lagos", "abuja"
    ],
    
    # Tech hubs and financial centers
    "Taiwan": [
        "taiwan", "taiwanese", "taipei"
    ],
    
    "Hong Kong": [
        "hong kong", "hk"
    ],
    
    # Add more countries as needed
}

# Helper functions
def get_all_countries() -> List[str]:
    """Get list of all country names"""
    return list(COUNTRIES_MAP.keys())

def get_country_aliases(country: str) -> List[str]:
    """Get aliases for a specific country"""
    return COUNTRIES_MAP.get(country, [])

def find_country_by_alias(alias: str) -> str:
    """Find country name by alias"""
    alias_lower = alias.lower()
    for country, aliases in COUNTRIES_MAP.items():
        if alias_lower in [a.lower() for a in aliases]:
            return country
    return ""

# Quick stats
def get_countries_stats() -> Dict[str, float]:
    """Get statistics about countries mapping"""
    return {
        "total_countries": float(len(COUNTRIES_MAP)),
        "total_aliases": float(sum(len(aliases) for aliases in COUNTRIES_MAP.values())),
        "avg_aliases_per_country": sum(len(aliases) for aliases in COUNTRIES_MAP.values()) / len(COUNTRIES_MAP)
    }

if __name__ == "__main__":
    stats = get_countries_stats()
    print(f"Countries mapping loaded: {stats['total_countries']} countries, {stats['total_aliases']} aliases")
    print(f"Average aliases per country: {stats['avg_aliases_per_country']:.1f}")
    
    # Test detection
    test_aliases = ["usa", "india", "uk", "china"]
    for alias in test_aliases:
        country = find_country_by_alias(alias)
        print(f"'{alias}' -> {country}")
