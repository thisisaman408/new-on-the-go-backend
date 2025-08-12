"""
Content processing and deduplication service
Enhances articles with metadata and removes duplicates
"""

import hashlib
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
import logging
from collections import Counter

from app.models.article import Article
from app.database import AsyncSessionLocal
from app.utils.text_cleaner import TextCleaner
from app.utils.hash_generator import (
    generate_content_hash,
    generate_batch_hashes,
    verify_hash_collision,
    normalize_text_for_hash
)
from app.services.deduplicator import ArticleDeduplicator, deduplicate_articles
from app.data.countries import COUNTRIES_MAP
from app.data.topic_keywords import TOPIC_KEYWORDS
from sqlalchemy import select, func, and_, or_
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

class ContentProcessor:
    """Advanced content processing and enhancement"""
    
    def __init__(self):
        self.processed_count = 0
        self.deduplicated_count = 0
        self.enhanced_count = 0
    
    async def process_unprocessed_articles(self, batch_size: int = 100) -> Dict[str, Any]:
        """
        Process articles that haven't been enhanced yet
        
        Args:
            batch_size: Number of articles to process in each batch
            
        Returns:
            Processing statistics
        """
        start_time = datetime.utcnow()
        total_processed = 0
        
        while True:
            # Get batch of unprocessed articles
            articles = await self._get_unprocessed_articles(batch_size)
            if not articles:
                break
            
            # Process the batch
            batch_results = await self._process_article_batch(articles)
            total_processed += len(articles)
            
            logger.info(f"Processed batch of {len(articles)} articles")
        
        # Run deduplication using the dedicated deduplicator service
        dedupe_stats = await self._run_enhanced_deduplication()
        
        end_time = datetime.utcnow()
        
        stats = {
            "articles_processed": total_processed,
            "processing_time_seconds": (end_time - start_time).total_seconds(),
            "deduplication_stats": dedupe_stats,
            "enhanced_articles": self.enhanced_count
        }
        
        logger.info(f"Content processing completed: {stats}")
        return stats
    
    async def _get_unprocessed_articles(self, limit: int) -> List[Article]:
        """Get articles that need processing"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Article)
                .filter(Article.content_processed.is_(False)) # type: ignore[attr-defined]
                .order_by(Article.discovered_at.desc())
                .limit(limit)
            )
            return list(result.scalars().all())
    
    async def _process_article_batch(self, articles: List[Article]) -> Dict[str, Any]:
        """Process a batch of articles with hash_generator integration"""
        async with AsyncSessionLocal() as session:
            enhanced_count = 0
            
            # Prepare data for batch hash generation
            articles_data = []
            for article in articles:
                articles_data.append({
                    'title': getattr(article, 'title', '') or '',
                    'url': getattr(article, 'url', '') or '',
                    'content': getattr(article, 'content', '') or ''
                })
            
            # Generate hashes in batch using hash_generator
            if articles_data:
                batch_hashes = generate_batch_hashes(articles_data)
            else:
                batch_hashes = {}
            
            for i, article in enumerate(articles):
                try:
                    # Get fresh article instance
                    result = await session.execute(
                        select(Article).filter(Article.id == article.id)
                    )
                    fresh_article = result.scalar_one()
                    
                    # Update content hash using hash_generator
                    if i in batch_hashes:
                        new_hash = batch_hashes[i]
                        from sqlalchemy.sql.schema import Column
                        from sqlalchemy.orm.attributes import InstrumentedAttribute
                        from sqlalchemy.sql.elements import ColumnElement
                        content_hash_value = fresh_article.content_hash
                        if isinstance(content_hash_value, (InstrumentedAttribute, Column, ColumnElement)):
                            content_hash_value = None
                        if content_hash_value != new_hash:
                            from sqlalchemy.sql.schema import Column
                            from sqlalchemy.orm.attributes import InstrumentedAttribute
                            if isinstance(getattr(type(fresh_article), "content_hash", None), (InstrumentedAttribute, Column)):
                                setattr(fresh_article, "content_hash", new_hash)
                            else:
                                setattr(fresh_article, "content_hash", new_hash)
                    else:
                        # Fallback to individual hash generation
                        new_hash = generate_content_hash(
                            getattr(fresh_article, 'title', '') or '',
                            getattr(fresh_article, 'url', '') or '',
                            getattr(fresh_article, 'content', '') or ''
                        )
                        # Convert SQLAlchemy column value to Python value before comparison
                        from sqlalchemy.orm.attributes import InstrumentedAttribute
                        from sqlalchemy.sql.schema import Column
                        from sqlalchemy.sql.elements import ColumnElement
                        content_hash_val = fresh_article.content_hash
                        if isinstance(content_hash_val, (InstrumentedAttribute, Column, ColumnElement)):
                            content_hash_val = None
                        if content_hash_val != new_hash:
                            setattr(fresh_article, "content_hash", new_hash)
                    
                    # Enhance article content
                    was_enhanced = await self._enhance_article_metadata(fresh_article)
                    if was_enhanced:
                        enhanced_count += 1
                    
                    # Mark as processed
                    fresh_article.content_processed = True
                    fresh_article.processed_at = datetime.utcnow()
                    
                except Exception as e:
                    logger.error(f"Error processing article {article.id}: {e}")
                    continue
            
            await session.commit()
            self.enhanced_count += enhanced_count
            
            return {"enhanced": enhanced_count}
    
    async def _enhance_article_metadata(self, article: Article) -> bool:
        """Enhance article with additional metadata"""
        enhanced = False
        
        try:
            # Enhance topic classification
            if self._enhance_topic_classification(article):
                enhanced = True
            
            # Extract geographic information
            if self._extract_geographic_data(article):
                enhanced = True
            
            # Detect importance level
            if self._classify_importance_level(article):
                enhanced = True
            
            # Extract stock symbols and business entities
            if self._extract_business_entities(article):
                enhanced = True
            
            # Calculate quality score
            if self._calculate_quality_score(article):
                enhanced = True
            
            # Generate better summary if needed
            if self._enhance_summary(article):
                enhanced = True
                
        except Exception as e:
            logger.error(f"Error enhancing article {article.id}: {e}")
        
        return enhanced
    
    def _enhance_topic_classification(self, article: Article) -> bool:
        """Enhance topic classification using keywords"""
        if (getattr(article, "content", None) is None or getattr(article, "content", "") == "") and \
           (getattr(article, "title", None) is None or getattr(article, "title", "") == ""):
            return False
        
        text_to_analyze = f"{article.title or ''} {article.content or ''}"[:1000].lower()
        
        # Score topics based on keyword matches
        topic_scores = {}
        for topic, keywords in TOPIC_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword.lower() in text_to_analyze)
            if score > 0:
                topic_scores[topic] = score
        
        if not topic_scores:
            return False
        
        # Get top topics
        sorted_topics = sorted(topic_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Update primary topic if we found a better match
        new_primary_topic = sorted_topics[0][0]
        if new_primary_topic != article.primary_topic:
            article.primary_topic = new_primary_topic
            
            # Update secondary topics
            secondary_topics = [topic for topic, score in sorted_topics[1:4]]  # Top 3 secondary
            if secondary_topics:
                article.secondary_topics = secondary_topics
                
            return True
        
        return False
    
    def _extract_geographic_data(self, article: Article) -> bool:
        """Extract countries and regions mentioned in article"""
        if (getattr(article, "content", None) is None or getattr(article, "content", "") == "") and \
           (getattr(article, "title", None) is None or getattr(article, "title", "") == ""):
            return False
        
        text_to_analyze = f"{article.title or ''} {article.content or ''}"[:2000].lower()
        
        # Find country mentions
        countries_found = set()
        for country, aliases in COUNTRIES_MAP.items():
            for alias in aliases:
                if alias.lower() in text_to_analyze:
                    countries_found.add(country)
        
        if countries_found:
            countries_value = article.countries_mentioned
            if not isinstance(countries_value, list):
                # Fix: handle SQLAlchemy Column type
                from sqlalchemy.orm.attributes import InstrumentedAttribute
                if isinstance(countries_value, InstrumentedAttribute):
                    countries_value = []
                else:
                    # If countries_value is a SQLAlchemy Column, set to empty list
                    from sqlalchemy.orm.attributes import InstrumentedAttribute
                    if isinstance(countries_value, InstrumentedAttribute):
                        countries_value = []
                    else:
                        from sqlalchemy.orm.attributes import InstrumentedAttribute
                        from sqlalchemy.sql.schema import Column
                        if countries_value is not None and not isinstance(countries_value, (InstrumentedAttribute, Column)):
                            countries_value = list(countries_value)
                        else:
                            countries_value = []
            current_countries = set(countries_value)
            new_countries = countries_found - current_countries
            
            if new_countries:
                article.countries_mentioned = list(current_countries | countries_found)
                return True
        
        return False
    
    def _classify_importance_level(self, article: Article) -> bool:
        """Classify article importance based on content and source"""
        if (getattr(article, "title", None) is None or getattr(article, "title", "") == "") and \
           (getattr(article, "content", None) is None or getattr(article, "content", "") == ""):
            return False
        
        text_to_analyze = f"{article.title or ''} {article.content or ''}"[:500].lower()
        
        # Breaking news indicators
        breaking_keywords = [
            'breaking', 'urgent', 'alert', 'just in', 'developing',
            'exclusive', 'emergency', 'crisis', 'disaster', 'tragedy'
        ]
        
        # Important news indicators
        important_keywords = [
            'major', 'significant', 'historic', 'unprecedented', 
            'announcement', 'decision', 'ruling', 'verdict'
        ]
        
        # Count keyword matches
        breaking_score = sum(1 for keyword in breaking_keywords if keyword in text_to_analyze)
        important_score = sum(1 for keyword in important_keywords if keyword in text_to_analyze)
        
        # Determine importance level
        new_importance = 'regular'
        
        source_reliability = getattr(article, "source_reliability", 0)
        if breaking_score >= 2 or (breaking_score >= 1 and source_reliability >= 90):
            new_importance = 'breaking'
        elif important_score >= 2 or (important_score >= 1 and breaking_score >= 1):
            new_importance = 'important'
        
        if hasattr(article, "importance_level") and new_importance != getattr(article, "importance_level", None):
            setattr(article, "importance_level", new_importance)
            return True
        
        return False
    
    def _extract_business_entities(self, article: Article) -> bool:
        """Extract stock symbols and business entities"""
        if (getattr(article, "content", None) is None or getattr(article, "content", "") == "") and \
           (getattr(article, "title", None) is None or getattr(article, "title", "") == ""):
            return False
        
        text_to_analyze = f"{article.title or ''} {article.content or ''}"
        
        # Extract potential stock symbols (3-5 letter codes in caps)
        stock_pattern = r'\b[A-Z]{3,5}\b'
        potential_stocks = set(re.findall(stock_pattern, text_to_analyze))
        
        # Filter out common non-stock words
        non_stock_words = {
            'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER',
            'WAS', 'ONE', 'OUR', 'HAD', 'HAS', 'TWO', 'WHO', 'ITS', 'DID', 'GET',
            'USA', 'CEO', 'CTO', 'CFO', 'COO', 'API', 'URL', 'PDF', 'HTML', 'CSS'
        }
        
        stock_symbols = [stock for stock in potential_stocks 
                        if stock not in non_stock_words and len(stock) <= 5]
        
        # Detect market sector
        sector_keywords = {
            'Technology': ['tech', 'software', 'ai', 'digital', 'app', 'platform'],
            'Finance': ['bank', 'finance', 'investment', 'loan', 'credit'],
            'Healthcare': ['health', 'medical', 'pharma', 'drug', 'hospital'],
            'Energy': ['oil', 'gas', 'energy', 'renewable', 'solar'],
            'Retail': ['retail', 'store', 'shopping', 'consumer', 'brand']
        }
        
        text_lower = text_to_analyze.lower()
        detected_sector = None
        max_score = 0
        
        for sector, keywords in sector_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > max_score:
                max_score = score
                detected_sector = sector
        
        # Update article
        updated = False
        
        # Ensure article.stock_symbols is a list before comparison
        stock_symbols_value = article.stock_symbols
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        if isinstance(stock_symbols_value, (InstrumentedAttribute, Column)) or stock_symbols_value is None:
            stock_symbols_value = []
        if stock_symbols and (not stock_symbols_value or set(stock_symbols) != set(stock_symbols_value)):
            # Assign to the underlying attribute if stock_symbols is a Column
            if hasattr(article, "_stock_symbols") or not isinstance(getattr(type(article), "stock_symbols", None), property):
                setattr(article, "stock_symbols", stock_symbols[:10])  # For hybrid or mapped property
            else:
                # If stock_symbols is a Column, assign to the appropriate attribute
                setattr(article, "stock_symbols", stock_symbols[:10])
            updated = True
        
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        if detected_sector:
            current_sector = article.market_sector
            # If market_sector is a Column or InstrumentedAttribute, assign to the underlying attribute
            if isinstance(current_sector, (InstrumentedAttribute, Column)):
                setattr(article, "market_sector", detected_sector)
                updated = True
            elif detected_sector != current_sector:
                setattr(article, "market_sector", detected_sector)
                updated = True
        
        return updated
    
    def _calculate_quality_score(self, article: Article) -> bool:
        """Calculate content quality score"""
        score = 0.0
        
        # Content length score (0-30 points)
        if getattr(article, "content", None) is not None and getattr(article, "content", "") != "":
            content = article.content
            # If content is a SQLAlchemy Column, treat as empty string
            from sqlalchemy.orm.attributes import InstrumentedAttribute
            from sqlalchemy.sql.schema import Column
            if isinstance(content, (InstrumentedAttribute, Column)):
                content = ""
            content_length = len(content)
            if content_length >= 1000:
                score += 30
            elif content_length >= 500:
                score += 20
            elif content_length >= 200:
                score += 10
        
        # Source reliability (0-25 points)
        reliability = article.source_reliability or 50
        score += (reliability / 100) * 25
        
        # Title quality (0-15 points)
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        title = article.title
        if title is not None and not isinstance(title, (InstrumentedAttribute, Column)) and title != "":
            title_length = len(title)
            if 30 <= title_length <= 100:
                score += 15
            elif 20 <= title_length <= 120:
                score += 10
            elif title_length >= 10:
                score += 5
        
        # Publication recency (0-15 points)
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        published_at = article.published_at
        if published_at is not None and not isinstance(published_at, (InstrumentedAttribute, Column)):
            hours_ago = (datetime.utcnow() - published_at.replace(tzinfo=None)).total_seconds() / 3600
            if hours_ago <= 1:
                score += 15
            elif hours_ago <= 6:
                score += 10
            elif hours_ago <= 24:
                score += 5
        
        # Topic classification (0-10 points)
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        primary_topic = article.primary_topic
        if primary_topic is not None and not isinstance(primary_topic, (InstrumentedAttribute, Column)):
            if primary_topic != 'general':
                score += 10
            else:
                score += 5
        
        # Geographic relevance (0-5 points)
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        countries_mentioned = article.countries_mentioned
        if isinstance(countries_mentioned, list) and len(countries_mentioned) > 0:
            score += 5
        
        # Normalize to 0-100
        # Ensure score is a float before calling min
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        from sqlalchemy.sql.elements import ColumnElement
        if isinstance(score, (InstrumentedAttribute, Column, ColumnElement)):
            score = 0.0
        try:
            score_float = float(score)
        except (TypeError, ValueError):
            score_float = 0.0
        final_score = min(100.0, score_float)
        
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        try:
            if isinstance(article.quality_score, (InstrumentedAttribute, Column)):
                article_quality_score = 0.0
            else:
                article_quality_score = float(article.quality_score or 0)
        except (TypeError, ValueError):
            article_quality_score = 0.0
        if abs(final_score - article_quality_score) > 1:
            # Assign using setattr if quality_score is a Column or InstrumentedAttribute
            if isinstance(getattr(type(article), "quality_score", None), (InstrumentedAttribute, Column)):
                setattr(article, "quality_score", final_score)
            else:
                setattr(article, "quality_score", final_score)
            return True
        
        return False
    
    def _enhance_summary(self, article: Article) -> bool:
        """Generate or enhance article summary"""
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        content = article.content
        if content is None or isinstance(content, (InstrumentedAttribute, Column)) or not isinstance(content, str) or len(content) < 300:
            return False
        
        # Generate new summary if missing or poor quality
        current_summary = article.summary or ""
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        from sqlalchemy.sql.schema import Column
        if not isinstance(current_summary, str) or isinstance(current_summary, (InstrumentedAttribute, Column)):
            current_summary = ""
        
        if len(current_summary) < 50 or len(current_summary) > 400:
            # Ensure article.content is a string before passing to extract_summary
            from sqlalchemy.orm.attributes import InstrumentedAttribute
            from sqlalchemy.sql.schema import Column
            content_value = article.content
            if isinstance(content_value, (InstrumentedAttribute, Column)) or not isinstance(content_value, str):
                content_value = ""
            new_summary = TextCleaner.extract_summary(content_value, max_length=300)
            if new_summary and new_summary != current_summary:
                from sqlalchemy.orm.attributes import InstrumentedAttribute
                from sqlalchemy.sql.schema import Column
                if isinstance(getattr(type(article), "summary", None), (InstrumentedAttribute, Column)):
                    setattr(article, "summary", new_summary)
                else:
                    setattr(article, "summary", new_summary)
                return True
        
        return False
    
    async def _run_enhanced_deduplication(self, days_back: int = 3) -> Dict[str, Any]:
        """Use the dedicated ArticleDeduplicator service for comprehensive deduplication"""
        try:
            # Use the standalone deduplicator service for better deduplication
            deduplicator = ArticleDeduplicator()
            
            # Run multiple deduplication strategies
            hash_stats = await deduplicator.deduplicate_by_content_hash(days_back)
            title_stats = await deduplicator.deduplicate_by_title_similarity(days_back=days_back)
            
            # Combine results
            total_removed = hash_stats.get('duplicates_removed', 0) + title_stats.get('duplicates_removed', 0)
            total_processed = max(hash_stats.get('articles_processed', 0), title_stats.get('articles_processed', 0))
            
            self.deduplicated_count = total_removed
            
            combined_stats = {
                "duplicates_removed": total_removed,
                "articles_checked": total_processed,
                "hash_based_removed": hash_stats.get('duplicates_removed', 0),
                "title_similarity_removed": title_stats.get('duplicates_removed', 0),
                "processing_time_seconds": hash_stats.get('processing_time_seconds', 0) + title_stats.get('processing_time_seconds', 0)
            }
            
            logger.info(f"Enhanced deduplication completed: {combined_stats}")
            return combined_stats
            
        except Exception as e:
            logger.error(f"Enhanced deduplication failed: {e}")
            # Fallback to basic deduplication
            return await self._fallback_deduplication(days_back)
    
    async def _fallback_deduplication(self, days_back: int = 3) -> Dict[str, Any]:
        """Fallback deduplication method if ArticleDeduplicator fails"""
        async with AsyncSessionLocal() as session:
            # Get recent articles for deduplication
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            
            result = await session.execute(
                select(Article)
                .filter(Article.discovered_at >= cutoff_date)
                .order_by(Article.discovered_at.desc())
            )
            articles = list(result.scalars().all())
            
            if len(articles) < 2:
                return {"duplicates_removed": 0, "articles_checked": len(articles)}
            
            # Find duplicates using hash collision verification from hash_generator
            duplicates_to_remove = []
            seen_hashes = set()
            
            for article in articles:
                if article.content_hash in seen_hashes:
                    duplicates_to_remove.append(article)
                else:
                    seen_hashes.add(article.content_hash)
            
            # Remove duplicates
            removed_count = 0
            for article in duplicates_to_remove:
                try:
                    await session.delete(article)
                    removed_count += 1
                except Exception as e:
                    logger.error(f"Error removing duplicate article {article.id}: {e}")
            
            await session.commit()
            
            logger.info(f"Fallback deduplication removed {removed_count} duplicate articles from {len(articles)} checked")
            
            return {
                "duplicates_removed": removed_count,
                "articles_checked": len(articles)
            }


# Convenience function
async def process_articles() -> Dict[str, Any]:
    """Process unprocessed articles"""
    processor = ContentProcessor()
    return await processor.process_unprocessed_articles()


# Testing function
if __name__ == "__main__":
    import asyncio
    
    async def test_processing():
        stats = await process_articles()
        print("Processing results:")
        print(f"Articles processed: {stats['articles_processed']}")
        print(f"Processing time: {stats['processing_time_seconds']:.2f}s")
        print(f"Enhanced articles: {stats['enhanced_articles']}")
        print(f"Duplicates removed: {stats['deduplication_stats']['duplicates_removed']}")
    
    asyncio.run(test_processing())
