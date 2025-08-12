"""
Standalone content deduplication service
Works independently or integrates with ContentProcessor for article deduplication
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Optional, Tuple
import logging
from collections import defaultdict

from app.models.article import Article
from app.database import AsyncSessionLocal
from app.utils.hash_generator import (
    generate_content_hash, 
    generate_similarity_hash,
    verify_hash_collision,
    calculate_hash_quality_metrics
)
from sqlalchemy import select, func, delete, or_, and_
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

class ArticleDeduplicator:
    """Standalone article deduplication service"""
    
    def __init__(self):
        self.duplicates_found = 0
        self.duplicates_removed = 0
        self.articles_processed = 0

    async def deduplicate_recent_articles(
        self, 
        days_back: int = 7,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Remove duplicate articles from recent content
        
        Args:
            days_back: How many days back to check for duplicates
            batch_size: Process articles in batches
            
        Returns:
            Deduplication statistics
        """
        start_time = datetime.utcnow()
        
        # Get recent articles for deduplication
        recent_articles = await self._get_recent_articles(days_back)
        self.articles_processed = len(recent_articles)
        
        if len(recent_articles) < 2:
            return self._create_stats_response(start_time, "No articles to deduplicate")
        
        logger.info(f"Starting deduplication for {len(recent_articles)} recent articles")
        
        # Process in batches for large datasets
        total_removed = 0
        
        for i in range(0, len(recent_articles), batch_size):
            batch = recent_articles[i:i + batch_size]
            batch_removed = await self._process_batch_for_duplicates(batch)
            total_removed += batch_removed
            
            logger.info(f"Processed batch {i//batch_size + 1}: removed {batch_removed} duplicates")
        
        self.duplicates_removed = total_removed
        
        return self._create_stats_response(start_time, f"Removed {total_removed} duplicates")

    async def deduplicate_by_content_hash(self, days_back: int = 3) -> Dict[str, Any]:
        """
        Remove exact duplicates based on content hash
        
        Args:
            days_back: Days to look back for duplicates
            
        Returns:
            Deduplication statistics
        """
        start_time = datetime.utcnow()
        
        async with AsyncSessionLocal() as session:
            # Find articles with duplicate content hashes
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            
            # Get duplicate hash groups
            duplicate_hashes_result = await session.execute(
                select(Article.content_hash, func.count(Article.id).label('count'))
                .filter(Article.discovered_at >= cutoff_date)
                .group_by(Article.content_hash)
                .having(func.count(Article.id) > 1)
            )
            
            duplicate_hashes = [row.content_hash for row in duplicate_hashes_result.fetchall()]
            
            if not duplicate_hashes:
                return self._create_stats_response(start_time, "No hash-based duplicates found")
            
            logger.info(f"Found {len(duplicate_hashes)} content hashes with duplicates")
            
            # Remove duplicates for each hash group
            total_removed = 0
            
            for content_hash in duplicate_hashes:
                removed = await self._remove_duplicates_by_hash(session, content_hash)
                total_removed += removed
            
            await session.commit()
            
            self.duplicates_removed = total_removed
            
            return self._create_stats_response(start_time, f"Hash-based: removed {total_removed} duplicates")

    async def deduplicate_by_title_similarity(
        self, 
        similarity_threshold: float = 0.85,
        days_back: int = 7
    ) -> Dict[str, Any]:
        """
        Remove duplicates based on title similarity
        
        Args:
            similarity_threshold: Similarity threshold (0.0 to 1.0)
            days_back: Days to look back
            
        Returns:
            Deduplication statistics
        """
        start_time = datetime.utcnow()
        
        # Get recent articles
        recent_articles = await self._get_recent_articles(days_back)
        
        if len(recent_articles) < 2:
            return self._create_stats_response(start_time, "Insufficient articles for similarity check")
        
        # Group by normalized titles
        title_groups = self._group_by_title_similarity(recent_articles)
        
        # Remove duplicates from similar title groups
        total_removed = await self._remove_similar_title_duplicates(title_groups)
        
        self.duplicates_removed = total_removed
        
        return self._create_stats_response(start_time, f"Title similarity: removed {total_removed} duplicates")

    async def deduplicate_by_url_domain(self, days_back: int = 1) -> Dict[str, Any]:
        """
        Remove duplicates from same domain (cross-posting detection)
        
        Args:
            days_back: Days to look back
            
        Returns:
            Deduplication statistics
        """
        start_time = datetime.utcnow()
        
        recent_articles = await self._get_recent_articles(days_back)
        
        # Group by domain and find cross-posted content
        domain_groups = defaultdict(list)
        
        for article in recent_articles:
            domain = self._extract_domain(getattr(article, "url", "") or "")
            if domain:
                domain_groups[domain].append(article)
        
        # Find and remove cross-domain duplicates
        total_removed = 0
        
        for domain, articles in domain_groups.items():
            if len(articles) > 1:
                removed = await self._remove_domain_duplicates(articles)
                total_removed += removed
        
        self.duplicates_removed = total_removed
        
        return self._create_stats_response(start_time, f"Domain-based: removed {total_removed} duplicates")

    async def regenerate_missing_hashes(self) -> Dict[str, Any]:
        """
        Regenerate content hashes for articles missing them
        
        Returns:
            Processing statistics
        """
        start_time = datetime.utcnow()
        
        async with AsyncSessionLocal() as session:
            # Find articles with missing or empty content hashes
            result = await session.execute(
                select(Article)
                .filter(or_(Article.content_hash.is_(None), Article.content_hash == ''))
                .order_by(Article.discovered_at.desc())
                .limit(1000)  # Process in chunks
            )
            
            articles_without_hash = list(result.scalars().all())
            
            if not articles_without_hash:
                return self._create_stats_response(start_time, "All articles have content hashes")
            
            logger.info(f"Regenerating hashes for {len(articles_without_hash)} articles")
            
            # Regenerate hashes
            regenerated_count = 0
            
            for article in articles_without_hash:
                try:
                    new_hash = generate_content_hash(
                        getattr(article, "title", "") or "",
                        getattr(article, "url", "") or "",
                        getattr(article, "content", "") or ""
                    )
                    article.content_hash = new_hash # type: ignore[assignment]
                    regenerated_count += 1 
                except Exception as e:
                    logger.error(f"Error regenerating hash for article {article.id}: {e}")
                    continue
            
            await session.commit()
            
            return self._create_stats_response(
                start_time, 
                f"Regenerated hashes for {regenerated_count} articles"
            )

    # Private helper methods
    
    async def _get_recent_articles(self, days_back: int) -> List[Article]:
        """Get recent articles for deduplication"""
        async with AsyncSessionLocal() as session:
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            
            result = await session.execute(
                select(Article)
                .filter(Article.discovered_at >= cutoff_date)
                .order_by(Article.discovered_at.desc())
            )
            
            return list(result.scalars().all())

    async def _process_batch_for_duplicates(self, articles: List[Article]) -> int:
        """Process a batch of articles for deduplication"""
        # Group by content hash
        hash_groups = defaultdict(list)
        
        for article in articles:
            if article.content_hash: # type: ignore[assignment]
                hash_groups[article.content_hash].append(article)
        
        # Remove duplicates from each group
        total_removed = 0
        
        async with AsyncSessionLocal() as session:
            for content_hash, duplicate_articles in hash_groups.items():
                if len(duplicate_articles) > 1:
                    # Keep the best article, remove others
                    best_article = self._select_best_article(duplicate_articles)
                    
                    for article in duplicate_articles:
                        if getattr(article, "id", None) != getattr(best_article, "id", None):
                            try:
                                await session.delete(article)
                                total_removed += 1
                            except Exception as e:
                                logger.error(f"Error removing duplicate article {article.id}: {e}")
            
            await session.commit()
        
        return total_removed

    async def _remove_duplicates_by_hash(self, session, content_hash: str) -> int:
        """Remove duplicates for a specific content hash"""
        # Get all articles with this hash
        result = await session.execute(
            select(Article)
            .filter(Article.content_hash == content_hash)
            .order_by(Article.discovered_at.desc())
        )
        
        duplicate_articles = list(result.scalars().all())
        
        if len(duplicate_articles) <= 1:
            return 0
        
        # Keep the best article
        best_article = self._select_best_article(duplicate_articles)
        
        removed_count = 0
        
        for article in duplicate_articles:
            if getattr(article, "id", None) != getattr(best_article, "id", None):
                try:
                    await session.delete(article)
                    removed_count += 1
                except Exception as e:
                    logger.error(f"Error removing duplicate {article.id}: {e}")
        
        return removed_count

    def _group_by_title_similarity(self, articles: List[Article]) -> Dict[str, List[Article]]:
        """Group articles by title similarity"""
        title_groups = defaultdict(list)
        
        for article in articles:
            if getattr(article, "title", None) is not None and getattr(article, "title", "") != "":
                normalized_title = self._normalize_title_for_comparison(getattr(article, "title", ""))
                if normalized_title:
                    title_groups[normalized_title].append(article)
        
        # Only return groups with potential duplicates
        return {k: v for k, v in title_groups.items() if len(v) > 1}

    async def _remove_similar_title_duplicates(self, title_groups: Dict[str, List[Article]]) -> int:
        """Remove duplicates from similar title groups"""
        total_removed = 0
        
        async with AsyncSessionLocal() as session:
            for normalized_title, similar_articles in title_groups.items():
                if len(similar_articles) > 1:
                    best_article = self._select_best_article(similar_articles)
                    
                    for article in similar_articles:
                        if getattr(article, "id", None) != getattr(best_article, "id", None):
                            try:
                                await session.delete(article)
                                total_removed += 1
                            except Exception as e:
                                logger.error(f"Error removing similar article {article.id}: {e}")
            
            await session.commit()
        
        return total_removed

    async def _remove_domain_duplicates(self, articles: List[Article]) -> int:
        """Remove duplicates from same domain"""
        if len(articles) <= 1:
            return 0
        
        # Group by title similarity within domain
        title_groups = self._group_by_title_similarity(articles)
        
        return await self._remove_similar_title_duplicates(title_groups)

    def _select_best_article(self, articles: List[Article]) -> Article:
        """Select the best article from a group of duplicates"""
        def score_article(article: Article) -> float:
            score = 0.0
            
            # Source reliability (0-50 points)
            reliability = getattr(article, "source_reliability", None)
            if reliability is not None and not isinstance(reliability, float):
                reliability = float(reliability)
            score += (reliability if reliability is not None else 50) / 2

            # Content length (0-30 points)
            content_length = len(getattr(article, "content", "") or "")
            if content_length > 1000:
                score += 30
            elif content_length > 500:
                score += 20
            elif content_length > 200:
                score += 10

            # Quality score (0-20 points)
            quality_score = getattr(article, "quality_score", None)
            if quality_score is not None and quality_score is not False:
                if not isinstance(quality_score, float):
                    quality_score = float(quality_score)
                score += (quality_score / 100) * 20

            return float(score)
        
        return max(articles, key=score_article)

    def _normalize_title_for_comparison(self, title: str) -> Optional[str]:
        """Normalize title for similarity comparison"""
        if not title or len(title) < 15:
            return None
        
        import re
        
        # Basic normalization
        normalized = title.lower().strip()
        
        # Remove common prefixes
        normalized = re.sub(r'^(breaking|exclusive|update|alert):\s*', '', normalized)
        
        # Remove source suffixes
        normalized = re.sub(r'\s*-\s*[^-]+$', '', normalized)
        
        # Remove punctuation and normalize spaces
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized if len(normalized) >= 10 else None

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return ""
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return ""

    def _create_stats_response(self, start_time: datetime, message: str) -> Dict[str, Any]:
        """Create standardized statistics response"""
        end_time = datetime.utcnow()
        
        return {
            "duplicates_removed": self.duplicates_removed,
            "articles_processed": self.articles_processed,
            "duplicates_found": self.duplicates_found,
            "processing_time_seconds": (end_time - start_time).total_seconds(),
            "message": message,
            "timestamp": end_time.isoformat()
        }


# Convenience functions for easy integration

async def deduplicate_articles(method: str = "hash", days_back: int = 3) -> Dict[str, Any]:
    """
    Deduplicate articles using specified method
    
    Args:
        method: 'hash', 'title', 'domain', or 'all'
        days_back: Days to look back
        
    Returns:
        Deduplication statistics
    """
    deduplicator = ArticleDeduplicator()
    
    if method == "hash":
        return await deduplicator.deduplicate_by_content_hash(days_back)
    elif method == "title":
        return await deduplicator.deduplicate_by_title_similarity(days_back=days_back)
    elif method == "domain":
        return await deduplicator.deduplicate_by_url_domain(days_back)
    elif method == "all":
        # Run all methods
        results = {}
        results["hash"] = await deduplicator.deduplicate_by_content_hash(days_back)
        results["title"] = await deduplicator.deduplicate_by_title_similarity(days_back=days_back)
        results["domain"] = await deduplicator.deduplicate_by_url_domain(days_back)
        return results
    else:
        raise ValueError(f"Unknown deduplication method: {method}")

async def regenerate_article_hashes() -> Dict[str, Any]:
    """Regenerate missing content hashes"""
    deduplicator = ArticleDeduplicator()
    return await deduplicator.regenerate_missing_hashes()


# Testing and maintenance functions

if __name__ == "__main__":
    async def test_deduplication():
        """Test the deduplication service"""
        print("Testing article deduplication...")
        
        # Test hash-based deduplication
        print("\n1. Hash-based deduplication:")
        stats = await deduplicate_articles("hash", days_back=7)
        print(f"   Removed: {stats['duplicates_removed']} articles")
        print(f"   Processed: {stats['articles_processed']} articles")
        print(f"   Time: {stats['processing_time_seconds']:.2f}s")
        
        # Test title similarity deduplication
        print("\n2. Title similarity deduplication:")
        stats = await deduplicate_articles("title", days_back=7)
        print(f"   Removed: {stats['duplicates_removed']} articles")
        print(f"   Time: {stats['processing_time_seconds']:.2f}s")
        
        # Regenerate missing hashes
        print("\n3. Regenerating missing hashes:")
        stats = await regenerate_article_hashes()
        print(f"   Message: {stats['message']}")
        
        print("\n✅ Deduplication testing completed!")
    
    asyncio.run(test_deduplication())
