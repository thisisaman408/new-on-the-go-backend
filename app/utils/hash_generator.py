"""
Content hash generation utilities for article deduplication
Generates unique hashes used by both ContentProcessor and standalone Deduplicator
"""

import hashlib
import re
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

def generate_content_hash(title: str, url: str, content: str = "") -> str:
    """
    Generate unique hash for article deduplication
    
    Args:
        title: Article title
        url: Article URL  
        content: Article content (optional, primarily use title + URL)
        
    Returns:
        MD5 hash string for deduplication
    """
    # Normalize inputs for consistent hashing
    title_normalized = normalize_text_for_hash(title or "")
    url_normalized = normalize_url_for_hash(url or "")
    
    # Primary hash based on title + URL (most reliable for deduplication)
    hash_input = f"{title_normalized}||{url_normalized}"
    
    return hashlib.md5(hash_input.encode('utf-8')).hexdigest()

def generate_similarity_hash(content: str, algorithm: str = "sha256") -> str:
    """
    Generate hash for content similarity detection
    
    Args:
        content: Article content
        algorithm: Hash algorithm ('md5', 'sha256')
        
    Returns:
        Content similarity hash
    """
    if not content:
        return "00000000"
    
    # Normalize content for similarity detection
    normalized = normalize_content_for_similarity(content)
    
    # Generate hash based on algorithm
    if algorithm == "md5":
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()[:8]
    else:
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:8]

def generate_url_hash(url: str) -> str:
    """
    Generate hash from URL for deduplication
    
    Args:
        url: Article URL
        
    Returns:
        URL-based hash
    """
    if not url:
        return ""
    
    normalized_url = normalize_url_for_hash(url)
    return hashlib.md5(normalized_url.encode('utf-8')).hexdigest()

def normalize_text_for_hash(text: str) -> str:
    """
    Normalize text for consistent hashing
    
    Args:
        text: Input text
        
    Returns:
        Normalized text for hashing
    """
    if not text:
        return ""
    
    # Basic normalization
    normalized = text.lower().strip()
    
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Remove common punctuation that doesn't affect meaning
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Remove very common stop words that add noise
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    words = normalized.split()
    meaningful_words = [word for word in words if word not in stop_words and len(word) > 2]
    
    return ' '.join(meaningful_words)

def normalize_url_for_hash(url: str) -> str:
    """
    Normalize URL for consistent hashing
    
    Args:
        url: Article URL
        
    Returns:
        Normalized URL for hashing
    """
    if not url:
        return ""
    
    # Basic URL normalization
    normalized = url.strip().lower()
    
    # Remove query parameters and fragments for better deduplication
    normalized = re.sub(r'[?#].*$', '', normalized)
    
    # Remove trailing slashes
    normalized = normalized.rstrip('/')
    
    # Remove common tracking parameters
    tracking_params = ['utm_source', 'utm_medium', 'utm_campaign', 'ref', 'source']
    for param in tracking_params:
        normalized = re.sub(f'[?&]{param}=[^&]*', '', normalized)
    
    return normalized

def normalize_content_for_similarity(content: str) -> str:
    """
    Normalize content for similarity detection
    
    Args:
        content: Article content
        
    Returns:
        Normalized content for similarity hashing
    """
    if not content:
        return ""
    
    # Take first 1000 characters for similarity
    text_sample = content[:1000].lower()
    
    # Remove HTML tags if any remain
    text_sample = re.sub(r'<[^>]+>', '', text_sample)
    
    # Remove extra whitespace and normalize
    text_sample = re.sub(r'\s+', ' ', text_sample).strip()
    
    # Remove common article prefixes
    text_sample = re.sub(r'^(breaking|exclusive|update):\s*', '', text_sample)
    
    return text_sample

def generate_batch_hashes(articles_data: List[Dict[str, Any]]) -> Dict[int, str]:
    """
    Generate hashes for multiple articles efficiently
    
    Args:
        articles_data: List of article dictionaries with 'title', 'url', 'content'
        
    Returns:
        Dictionary mapping article index to content hash
    """
    hashes = {}
    
    for i, article_data in enumerate(articles_data):
        try:
            content_hash = generate_content_hash(
                article_data.get('title', ''),
                article_data.get('url', ''),
                article_data.get('content', '')
            )
            hashes[i] = content_hash
        except Exception as e:
            logger.error(f"Error generating hash for article {i}: {e}")
            hashes[i] = ""
    
    return hashes

def verify_hash_collision(hash1: str, hash2: str) -> bool:
    """
    Check if two hashes indicate potential collision
    
    Args:
        hash1: First hash
        hash2: Second hash
        
    Returns:
        True if hashes match (potential duplicate)
    """
    return hash1 == hash2 and hash1 != ""

def calculate_hash_quality_metrics(hashes: List[str]) -> Dict[str, Any]:
    """
    Calculate hash distribution metrics for collision analysis
    
    Args:
        hashes: List of generated hashes
        
    Returns:
        Hash quality metrics
    """
    if not hashes:
        return {"total_hashes": 0, "unique_hashes": 0, "collision_rate": 0.0}
    
    valid_hashes = [h for h in hashes if h and len(h) == 32]  # MD5 length
    unique_hashes = set(valid_hashes)
    
    # Calculate prefix distribution (first 4 chars)
    prefix_counts = {}
    for hash_val in valid_hashes:
        prefix = hash_val[:4]
        prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
    
    collision_rate = (len(valid_hashes) - len(unique_hashes)) / len(valid_hashes) if valid_hashes else 0
    
    return {
        "total_hashes": len(hashes),
        "valid_hashes": len(valid_hashes),
        "unique_hashes": len(unique_hashes),
        "collision_rate": collision_rate,
        "max_prefix_collision": max(prefix_counts.values()) if prefix_counts else 0,
        "prefix_distribution": len(prefix_counts)
    }

# Testing and validation functions
if __name__ == "__main__":
    # Test hash generation
    test_articles = [
        {"title": "OpenAI Launches New Model", "url": "https://example.com/news/1", "content": "OpenAI today..."},
        {"title": "OpenAI Launches New Model", "url": "https://example.com/news/1", "content": "OpenAI today..."},  # Duplicate
        {"title": "Different News", "url": "https://example.com/news/2", "content": "Different content..."},
    ]
    
    print("Testing hash generation:")
    for i, article in enumerate(test_articles):
        hash_val = generate_content_hash(article["title"], article["url"], article["content"])
        print(f"Article {i}: {hash_val}")
    
    # Test collision detection
    hash1 = generate_content_hash("Same Title", "https://example.com/same")
    hash2 = generate_content_hash("Same Title", "https://example.com/same")
    print(f"\nCollision test: {verify_hash_collision(hash1, hash2)}")
    
    # Test batch processing
    batch_hashes = generate_batch_hashes(test_articles)
    print(f"\nBatch hashes: {batch_hashes}")
    
    # Test quality metrics
    all_hashes = list(batch_hashes.values())
    metrics = calculate_hash_quality_metrics(all_hashes)
    print(f"\nHash quality metrics: {metrics}")
