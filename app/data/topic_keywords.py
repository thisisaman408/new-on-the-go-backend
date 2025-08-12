"""
Topic keywords for content classification
Maps topic names to relevant keywords and phrases
"""

from typing import Dict, List

# Comprehensive mapping of topics to classification keywords
TOPIC_KEYWORDS: Dict[str, List[str]] = {
    # General News
    "general": [
        "news", "breaking", "update", "report", "announcement", "latest", 
        "headline", "story", "coverage", "incident", "event"
    ],
    
    # Technology
    "technology": [
        "technology", "tech", "software", "hardware", "app", "application", 
        "platform", "digital", "internet", "web", "online", "cyber", "data", 
        "algorithm", "programming", "developer", "coding", "innovation", 
        "gadget", "device", "smartphone", "computer", "laptop", "tablet"
    ],
    
    # Business & Economy  
    "business": [
        "business", "company", "corporation", "enterprise", "firm", "industry", 
        "economy", "economic", "market", "finance", "financial", "revenue", 
        "profit", "earnings", "sales", "growth", "investment", "investor", 
        "banking", "bank", "trade", "trading", "commerce", "commercial", 
        "merger", "acquisition", "ipo", "ceo", "executive", "board"
    ],
    
    # Politics & Government
    "politics": [
        "politics", "political", "government", "minister", "prime minister", 
        "president", "election", "vote", "voting", "parliament", "congress", 
        "senate", "policy", "law", "legislation", "bill", "act", "regulation", 
        "democracy", "democratic", "republican", "party", "campaign", "debate"
    ],
    
    # Sports
    "sports": [
        "sports", "sport", "game", "match", "tournament", "championship", 
        "league", "team", "player", "athlete", "coach", "football", "soccer", 
        "cricket", "basketball", "tennis", "baseball", "hockey", "olympics", 
        "fifa", "nba", "nfl", "ipl", "premier league", "score", "win", "victory"
    ],
    
    # Entertainment
    "entertainment": [
        "entertainment", "movie", "film", "cinema", "bollywood", "hollywood", 
        "actor", "actress", "director", "music", "song", "album", "concert", 
        "tv", "television", "show", "series", "celebrity", "star", "award", 
        "oscar", "grammy", "festival", "premiere", "release"
    ],
    
    # Science & Research
    "science": [
        "science", "scientific", "research", "study", "discovery", "experiment", 
        "laboratory", "university", "academic", "scholar", "physics", "chemistry", 
        "biology", "medicine", "space", "nasa", "astronomy", "climate", 
        "environment", "vaccine", "drug", "treatment", "therapy"
    ],
    
    # Health & Medicine
    "health": [
        "health", "healthcare", "medical", "medicine", "hospital", "doctor", 
        "patient", "disease", "illness", "virus", "covid", "pandemic", 
        "vaccine", "vaccination", "treatment", "cure", "therapy", "diagnosis", 
        "wellness", "fitness", "nutrition", "diet", "mental health"
    ],
    
    # STOCKS (Your new category)
    "stocks": [
        "stock", "stocks", "share", "shares", "equity", "market", "stock market", 
        "trading", "trader", "investor", "investment", "portfolio", "dividend", 
        "nasdaq", "nyse", "nse", "bse", "nifty", "sensex", "dow jones", "s&p", 
        "bull", "bear", "rally", "crash", "volatility", "ipo", "listing", 
        "earnings", "quarterly", "revenue", "valuation", "price target"
    ],
    
    # STARTUPS (Your new category)
    "startups": [
        "startup", "startups", "entrepreneur", "entrepreneurship", "founder", 
        "co-founder", "venture", "venture capital", "vc", "funding", "investment", 
        "seed", "series a", "series b", "angel investor", "accelerator", 
        "incubator", "pitch", "demo day", "unicorn", "valuation", "exit", 
        "acquisition", "scale", "scaling", "bootstrap", "mvp", "product launch"
    ],
    
    # AI (Your new category)
    "ai": [
        "ai", "artificial intelligence", "machine learning", "ml", "deep learning", 
        "neural network", "algorithm", "automation", "robot", "robotics", 
        "chatbot", "nlp", "natural language processing", "computer vision", 
        "openai", "gpt", "chatgpt", "llm", "large language model", "tensorflow", 
        "pytorch", "data science", "big data", "analytics", "predictive", 
        "autonomous", "self-driving", "smart", "intelligent"
    ],
    
    # Additional specific topics for better classification
    "finance": [
        "finance", "financial", "bank", "banking", "loan", "credit", "debt", 
        "insurance", "mortgage", "interest", "rate", "federal reserve", "rbi", 
        "monetary", "fiscal", "budget", "tax", "taxation", "currency", "dollar", 
        "rupee", "euro", "bitcoin", "cryptocurrency", "forex"
    ],
    
    "energy": [
        "energy", "oil", "gas", "coal", "renewable", "solar", "wind", "nuclear", 
        "power", "electricity", "grid", "battery", "fuel", "petroleum", "opec", 
        "crude", "refinery", "pipeline", "carbon", "emission", "climate change"
    ],
    
    "automotive": [
        "car", "auto", "automobile", "vehicle", "electric vehicle", "ev", 
        "tesla", "toyota", "honda", "ford", "bmw", "mercedes", "automotive", 
        "driving", "self-driving", "autonomous", "uber", "lyft", "ride-sharing"
    ],
    
    "real estate": [
        "real estate", "property", "housing", "home", "house", "apartment", 
        "rent", "rental", "mortgage", "construction", "developer", "builder", 
        "commercial", "residential", "land", "plot", "investment property"
    ]
}

# Helper functions
def get_all_topics() -> List[str]:
    """Get list of all topic names"""
    return list(TOPIC_KEYWORDS.keys())

def get_topic_keywords(topic: str) -> List[str]:
    """Get keywords for a specific topic"""
    return TOPIC_KEYWORDS.get(topic, [])

def classify_text_by_keywords(text: str, min_matches: int = 1) -> Dict[str, int]:
    """
    Classify text by counting keyword matches for each topic
    
    Args:
        text: Text to classify
        min_matches: Minimum matches required to include topic
        
    Returns:
        Dictionary of topic -> match count
    """
    text_lower = text.lower()
    topic_scores = {}
    
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword.lower() in text_lower)
        if score >= min_matches:
            topic_scores[topic] = score
    
    return topic_scores

def get_top_topics(text: str, top_n: int = 3) -> List[tuple]:
    """
    Get top N topics for given text
    
    Returns:
        List of (topic, score) tuples sorted by score
    """
    scores = classify_text_by_keywords(text)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]

# Quick stats
def get_keywords_stats() -> Dict[str, float]:
    """Get statistics about topic keywords"""
    return {
        "total_topics": len(TOPIC_KEYWORDS),
        "total_keywords": sum(len(keywords) for keywords in TOPIC_KEYWORDS.values()),
        "avg_keywords_per_topic": sum(len(keywords) for keywords in TOPIC_KEYWORDS.values()) / len(TOPIC_KEYWORDS)
    }

if __name__ == "__main__":
    stats = get_keywords_stats()
    print(f"Topic keywords loaded: {stats['total_topics']} topics, {stats['total_keywords']} keywords")
    print(f"Average keywords per topic: {stats['avg_keywords_per_topic']:.1f}")
    
    # Test classification
    test_text = "OpenAI releases new ChatGPT model with improved artificial intelligence capabilities"
    top_topics = get_top_topics(test_text)
    print(f"\nClassification for: '{test_text}'")
    for topic, score in top_topics:
        print(f"  {topic}: {score} matches")
    
    # Show your new categories
    print(f"\nYour new categories:")
    print(f"  STOCKS: {len(get_topic_keywords('stocks'))} keywords")
    print(f"  STARTUPS: {len(get_topic_keywords('startups'))} keywords") 
    print(f"  AI: {len(get_topic_keywords('ai'))} keywords")
