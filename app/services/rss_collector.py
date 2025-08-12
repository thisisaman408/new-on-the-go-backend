"""
Comprehensive RSS collection service with all critical fixes applied
Handles fetching, parsing, and processing RSS feeds at scale with proper error handling
"""

import asyncio
import aiohttp
import feedparser
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse
import logging

from app.models.source import NewsSource
from app.models.article import Article
from app.database import AsyncSessionLocal
from app.utils.date_parser import parse_rss_date
from app.utils.text_cleaner import TextCleaner
from app.config import settings
from sqlalchemy import select, func, text
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

class SourceCircuitBreaker:
    """Circuit breaker pattern to prevent repeated failures from bad sources"""
    
    def __init__(self):
        self.failure_counts = {}
        self.disabled_until = {}
    
    def should_skip_source(self, source_id: int) -> bool:
        """Check if source should be temporarily disabled"""
        if source_id in self.disabled_until:
            if datetime.utcnow() < self.disabled_until[source_id]:
                return True
            else:
                # Re-enable source
                del self.disabled_until[source_id]
                self.failure_counts[source_id] = 0
        
        return False
    
    def record_failure(self, source_id: int):
        """Record source failure and potentially disable it"""
        self.failure_counts[source_id] = self.failure_counts.get(source_id, 0) + 1
        
        if self.failure_counts[source_id] >= 5:
            # Disable for 1 hour after 5 failures
            self.disabled_until[source_id] = datetime.utcnow() + timedelta(hours=1)
            logger.warning(f"Source {source_id} disabled for 1 hour due to repeated failures")
    
    def record_success(self, source_id: int):
        """Record source success and reset failure count"""
        self.failure_counts[source_id] = 0
        if source_id in self.disabled_until:
            del self.disabled_until[source_id]

class RSSCollector:
    """Scalable RSS collection service with comprehensive error handling"""
    
    def __init__(self):
        self.session = None
        self.collected_articles = 0 
        self.processed_sources = 0
        self.failed_sources = 0
        self.db_semaphore = asyncio.Semaphore(5)  # Limit concurrent DB operations
        self.circuit_breaker = SourceCircuitBreaker()
        
    async def __aenter__(self):
        """Async context manager entry with enhanced headers and compression support"""
        timeout = aiohttp.ClientTimeout(total=60, connect=20)
        
        # Enhanced headers with better compression support
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, text/html, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',  # Brotli support
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Create connector with compression support
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers=headers,
            connector=connector,
            auto_decompress=True  # Automatically handle gzip, deflate, and brotli
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def collect_from_all_sources(self, max_concurrent: Optional[int] = None) -> Dict[str, Any]:
        """
        Collect articles from all enabled RSS sources with comprehensive error handling
        
        Args:
            max_concurrent: Maximum concurrent source processing
            
        Returns:
            Collection statistics
        """
        max_concurrent = max_concurrent or settings.RSS_CONCURRENT_REQUESTS
        
        # Get sources that are due for polling
        sources = await self._get_sources_due_for_poll()
        logger.info(f"Found {len(sources)} sources due for polling")
        
        if not sources:
            return {
                "sources_processed": 0,
                "articles_collected": 0,
                "sources_failed": 0,
                "message": "No sources due for polling"
            }
        
        # Filter out circuit breaker disabled sources
        active_sources = [
            source for source in sources 
            if not self.circuit_breaker.should_skip_source(getattr(source, "id"))
        ]
        
        if len(active_sources) < len(sources):
            logger.info(f"Circuit breaker disabled {len(sources) - len(active_sources)} sources")
        
        # Process sources concurrently with semaphore
        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = [
            self._collect_from_source_with_semaphore(source, semaphore)
            for source in active_sources
        ]
        
        start_time = datetime.utcnow()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = datetime.utcnow()
        
        # Process results
        successful_collections = []
        failed_collections = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Source collection failed: {active_sources[i].name} - {result}")
                failed_collections.append(active_sources[i])
                self.circuit_breaker.record_failure(getattr(active_sources[i], "id"))
            else:
                successful_collections.append(result)
                if isinstance(result, dict) and result.get('articles_collected', 0) > 0:
                    self.circuit_breaker.record_success(getattr(active_sources[i], "id"))
        
        # Update statistics
        total_articles = sum(r.get('articles_collected', 0) for r in successful_collections if isinstance(r, dict))
        
        stats = {
            "sources_processed": len(successful_collections),
            "sources_failed": len(failed_collections),
            "articles_collected": total_articles,
            "processing_time_seconds": (end_time - start_time).total_seconds(),
            "successful_sources": [r.get('source_name') for r in successful_collections if isinstance(r, dict)],
            "failed_sources": [s.name for s in failed_collections],
            "circuit_breaker_disabled": len(sources) - len(active_sources)
        }
        
        logger.info(f"RSS collection completed: {stats}")
        return stats
    
    async def _fetch_rss_with_retry(self, source: NewsSource, max_retries: int = 3) -> Optional[str]:
        """Fetch RSS with enhanced retry logic and better error handling"""
        for attempt in range(max_retries):
            try:
                # Add source-specific headers if needed
                request_headers = {}
                if hasattr(source, 'custom_headers') and isinstance(source.custom_headers, dict):
                    request_headers.update(source.custom_headers)
                
                async with self.session.get(getattr(source, "url"), headers=request_headers) as response: #type: ignore[union-attr]
                    if response.status == 200:
                        # Successfully got content
                        content = await response.text()
                        logger.debug(f"Successfully fetched {len(content)} chars from {source.name}")
                        return content
                    elif response.status == 403:
                        logger.warning(f"403 Forbidden for {source.name} - may be blocked")
                        return None
                    elif response.status == 404:
                        logger.warning(f"404 Not Found for {source.name} - URL may have changed")
                        return None
                    else:
                        logger.warning(f"HTTP {response.status} for {source.name}")
                        
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Timeout for {source.name}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Final timeout for {source.name} after {max_retries} attempts")
                    return None
            except Exception as e:
                logger.error(f"Error fetching {source.name}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None
        
        return None

    async def _collect_from_source_with_semaphore(
        self, 
        source: NewsSource, 
        semaphore: asyncio.Semaphore
    ) -> Dict[str, Any]:
        """Collect from source with concurrency control"""
        async with semaphore:
            return await self._collect_from_source(source)
    
    async def _collect_from_source(self, source: NewsSource) -> Dict[str, Any]:
        """
        Collect articles from a single RSS source with enhanced content extraction
        
        Args:
            source: NewsSource model instance
            
        Returns:
            Collection results
        """
        start_time = datetime.utcnow()
        articles_collected = 0
        
        try:
            logger.info(f"Collecting from: {source.name} ({source.url})")
            
            # Fetch RSS feed with retry logic
            feed_data = await self._fetch_rss_with_retry(source)
            if not feed_data:
                await self._record_failed_poll(source, "Failed to fetch RSS feed")
                return {"source_name": source.name, "articles_collected": 0, "error": "Fetch failed"}
            
            # Parse feed
            feed = feedparser.parse(feed_data)
            if not feed.entries:
                await self._record_failed_poll(source, "No entries found in RSS feed")
                return {"source_name": source.name, "articles_collected": 0, "error": "No entries"}
            
            logger.info(f"Found {len(feed.entries)} entries in {source.name}")
            
            # Process entries with enhanced content extraction and batch duplicate checking
            articles_to_insert = await self._process_feed_entries_batch(
                feed.entries[:source.max_articles_per_poll or settings.MAX_ARTICLES_PER_FEED],
                source, 
                feed
            )
            
            # Bulk insert articles
            if articles_to_insert:
                inserted_count = await self._bulk_insert_articles(articles_to_insert)
                articles_collected = inserted_count
                logger.info(f"Successfully inserted {inserted_count} articles from {source.name}")
            
            # Record successful poll
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            await self._record_successful_poll(source, response_time, articles_collected)
            
            return {
                "source_name": source.name,
                "articles_collected": articles_collected,
                "total_entries": len(feed.entries),
                "response_time_ms": response_time
            }
            
        except Exception as e:
            logger.error(f"Error collecting from {source.name}: {e}")
            await self._record_failed_poll(source, str(e))
            return {"source_name": source.name, "articles_collected": 0, "error": str(e)}

    async def _process_feed_entries_batch(self, entries: List[Any], source: NewsSource, feed: Any) -> List[Dict[str, Any]]:
        """Process entries in batches to reduce DB calls"""
        articles_to_process = []
        content_hashes = []
        
        # First pass: extract all data
        for entry in entries:
            article_data = await self._process_feed_entry(entry, source, feed)
            if article_data:
                articles_to_process.append(article_data)
                content_hashes.append(article_data['content_hash'])
        
        # Batch check existing articles
        existing_hashes = await self._batch_check_existing_articles(content_hashes)
        
        # Filter out existing articles
        new_articles = [
            article for article in articles_to_process 
            if article['content_hash'] not in existing_hashes
        ]
        
        logger.debug(f"Processed {len(entries)} entries, {len(new_articles)} are new for {source.name}")
        return new_articles

    async def _batch_check_existing_articles(self, content_hashes: List[str]) -> Set[str]:
        """Check multiple articles at once instead of individual queries"""
        if not content_hashes:
            return set()
            
        async with self.db_semaphore:
            async with AsyncSessionLocal() as session:
                try:
                    result = await session.execute(
                        select(Article.content_hash).filter(
                            Article.content_hash.in_(content_hashes)
                        )
                    )
                    existing_hashes = {row.content_hash for row in result.fetchall()}
                    return existing_hashes
                except Exception as e:
                    logger.error(f"Error batch checking articles: {e}")
                    return set()
    
    async def _process_feed_entry(
        self, 
        entry: Any, 
        source: NewsSource, 
        feed: Any
    ) -> Optional[Dict[str, Any]]:
        """Process a single RSS feed entry with enhanced content extraction"""
        try:
            # Extract basic fields
            title = getattr(entry, 'title', '')
            link = getattr(entry, 'link', '')
            
            if not title or not link:
                logger.debug(f"Skipping entry without title or link from {source.name}")
                return None
            
            # Extract content using enhanced method
            content = self._extract_entry_content(entry)
            
            if not content or len(content.strip()) < 20:
                logger.debug(f"Skipping entry with insufficient content from {source.name}: {title[:50]}...")
                # Don't return None immediately, try to get some content from title/description
                content = title  # Fallback to title as content
            
            # Parse publication date
            published_at = self._extract_entry_date(entry)
            
            # Generate content hash for deduplication
            content_hash = self._generate_content_hash(title, link, content)
            
            # Clean content
            cleaned_data = TextCleaner.clean_rss_item({
                'title': title,
                'content': content,
                'url': link
            })
            
            # Build article data
            article_data = {
                'content_hash': content_hash,
                'title': cleaned_data.get('title', title)[:500],  # Limit title length
                'content': cleaned_data.get('content'),
                'url': link,
                'source_name': source.name,
                'source_url': source.url,
                'source_type': source.source_type,
                'source_reliability': source.reliability_score,
                'published_at': published_at,
                'discovered_at': datetime.utcnow(),
                'primary_region': source.primary_region,
                'language': source.language,
                'word_count': cleaned_data.get('word_count', 0),
                'reading_time_minutes': cleaned_data.get('reading_time_minutes', 1),
                'summary': cleaned_data.get('summary'),
                'primary_topic': self._classify_primary_topic(source, title, content),
                'secondary_topics': source.topics or [],
                'importance_level': 'regular',
                'content_processed': True,
                'meta_data': {
                    'feed_title': getattr(feed.feed, 'title', ''),
                    'entry_id': getattr(entry, 'id', ''),
                    'author': getattr(entry, 'author', ''),
                    'tags': [tag.term for tag in getattr(entry, 'tags', [])],
                }
            }
            
            logger.debug(f"Successfully processed article: {title[:50]}... ({len(content)} chars)")
            return article_data
            
        except Exception as e:
            logger.error(f"Error processing entry from {source.name}: {e}")
            return None
    
    def _extract_entry_content(self, entry: Any) -> str:
        """Extract content from RSS entry (try multiple fields) - ENHANCED VERSION"""
        content_candidates = []
        
        # Method 1: Handle content field (can be list or dict or string)
        if hasattr(entry, 'content') and entry.content:
            content_value = entry.content
            
            if isinstance(content_value, list) and content_value:
                # YourStory type: [{'type': 'text/html', 'value': '<div>...'}]
                for content_item in content_value:
                    if isinstance(content_item, dict):
                        if 'value' in content_item:
                            content_candidates.append(content_item['value'])
                        else:
                            content_candidates.append(str(content_item))
                    else:
                        content_candidates.append(str(content_item))
            elif isinstance(content_value, dict):
                if 'value' in content_value:
                    content_candidates.append(content_value['value'])
                else:
                    content_candidates.append(str(content_value))
            elif content_value:
                content_candidates.append(str(content_value))
        
        # Method 2: Try standard RSS fields in priority order
        standard_fields = ['description', 'summary', 'subtitle']
        for field in standard_fields:
            if hasattr(entry, field):
                field_value = getattr(entry, field)
                if field_value and isinstance(field_value, str) and len(field_value.strip()) > 20:
                    content_candidates.append(field_value)
        
        # Method 3: Try encoded content fields
        encoded_fields = ['content_encoded', 'encoded']
        for field in encoded_fields:
            if hasattr(entry, field) and getattr(entry, field):
                content_candidates.append(str(getattr(entry, field)))
        
        # Return the longest meaningful content
        if content_candidates:
            # Filter out very short content (likely just metadata)
            meaningful_content = [c for c in content_candidates if len(c.strip()) > 50]
            if meaningful_content:
                # Return the longest content (usually the most complete)
                return max(meaningful_content, key=len)
            else:
                # Fallback to any content if no long content found
                return max(content_candidates, key=len)
        
        return ""

    def _extract_entry_date(self, entry: Any) -> Optional[datetime]:
        """Extract publication date from RSS entry"""
        date_fields = ['published', 'updated', 'created', 'pubDate']
        
        for field in date_fields:
            if hasattr(entry, field):
                date_value = getattr(entry, field)
                parsed_date = parse_rss_date(date_value)
                if parsed_date:
                    return parsed_date
        
        # Try structured time fields
        for field in ['published_parsed', 'updated_parsed']:
            if hasattr(entry, field):
                time_struct = getattr(entry, field)
                if time_struct:
                    try:
                        return datetime(*time_struct[:6]).replace(tzinfo=None)
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def _generate_content_hash(self, title: str, url: str, content: str) -> str:
        """Generate unique hash for article deduplication"""
        hash_input = f"{title.strip().lower()}{url.strip()}"
        return hashlib.md5(hash_input.encode('utf-8')).hexdigest()
    
    def _classify_primary_topic(self, source: NewsSource, title: str, content: str) -> Optional[str]:
        """Basic topic classification based on source and keywords"""
        if isinstance(source.topics, list) and len(source.topics) > 0:
            return source.topics[0]
        
        combined_text = f"{title.lower()} {content.lower()}"[:500]
        
        tech_keywords = ['technology', 'tech', 'ai', 'software', 'startup', 'app', 'digital']
        business_keywords = ['business', 'economy', 'finance', 'market', 'company', 'stock']
        politics_keywords = ['politics', 'government', 'election', 'policy', 'minister', 'parliament']
        
        if any(keyword in combined_text for keyword in tech_keywords):
            return 'technology'
        elif any(keyword in combined_text for keyword in business_keywords):
            return 'business' 
        elif any(keyword in combined_text for keyword in politics_keywords):
            return 'politics'
        
        return 'general'
    
    async def _bulk_insert_articles(self, articles: List[Dict[str, Any]]) -> int:
        """Bulk insert with proper transaction isolation"""
        inserted_count = 0
        
        # Process in smaller batches to reduce lock contention
        batch_size = 5  # Smaller batches
        
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            
            async with self.db_semaphore:  # Limit concurrent DB operations
                async with AsyncSessionLocal() as session:
                    try:
                        # Use explicit transaction isolation
                        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                        
                        for article_data in batch:
                            try:
                                article = Article(**article_data)
                                session.add(article)
                            except Exception as e:
                                logger.warning(f"Failed to create article: {e}")
                                continue
                        
                        await session.commit()
                        inserted_count += len(batch)
                        logger.debug(f"Successfully inserted batch of {len(batch)} articles")
                        
                    except IntegrityError:
                        await session.rollback()
                        # Try individual inserts for this batch
                        individual_count = await self._individual_insert_articles(batch)
                        inserted_count += individual_count
                    except Exception as e:
                        await session.rollback()
                        logger.error(f"Batch insert failed: {e}")
                        continue
        
        return inserted_count

    async def _individual_insert_articles(self, articles: List[Dict[str, Any]]) -> int:
        """Insert articles individually (fallback method)"""
        inserted_count = 0
        async with self.db_semaphore:
            async with AsyncSessionLocal() as session:
                for article_data in articles:
                    try:
                        article = Article(**article_data)
                        session.add(article)
                        await session.commit()
                        inserted_count += 1
                    except IntegrityError:
                        await session.rollback()
                        continue
                    except Exception as e:
                        await session.rollback()
                        logger.warning(f"Failed to insert article: {e}")
                        continue
        
        return inserted_count
    
    async def _get_sources_due_for_poll(self) -> List[NewsSource]:
        """Get RSS sources that are due for polling"""
        async with AsyncSessionLocal() as session:
            now = datetime.utcnow()
            result = await session.execute(
                select(NewsSource).filter(
                    NewsSource.enabled.is_(True), # type: ignore[attr-defined]
                    NewsSource.next_poll_at <= now #type: ignore[attr-defined]
                ).order_by(NewsSource.reliability_score.desc()) # type: ignore[attr-defined]
            )
            return list(result.scalars().all())
    
    async def _record_successful_poll(
        self, 
        source: NewsSource, 
        response_time_ms: float, 
        articles_count: int
    ):
        """Record successful RSS poll"""
        async with self.db_semaphore:
            async with AsyncSessionLocal() as session:
                try:
                    result = await session.execute(select(NewsSource).filter(NewsSource.id == source.id))
                    fresh_source = result.scalar_one()
                    fresh_source.record_successful_poll(response_time_ms, articles_count)
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Error recording successful poll: {e}")
    
    async def _record_failed_poll(self, source: NewsSource, error_message: str):
        """Record failed RSS poll"""
        async with self.db_semaphore:
            async with AsyncSessionLocal() as session:
                try:
                    result = await session.execute(select(NewsSource).filter(NewsSource.id == source.id))
                    fresh_source = result.scalar_one()
                    fresh_source.record_failed_poll(error_message)
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Error recording failed poll: {e}")


# Convenience function for manual collection
async def collect_rss_articles(max_concurrent: Optional[int] = None) -> Dict[str, Any]:
    """Collect articles from all RSS sources"""
    async with RSSCollector() as collector:
        return await collector.collect_from_all_sources(max_concurrent)


# Testing function
if __name__ == "__main__":
    async def test_collection():
        stats = await collect_rss_articles(max_concurrent=3)
        print("Collection results:")
        print(f"Sources processed: {stats['sources_processed']}")
        print(f"Articles collected: {stats['articles_collected']}")
        print(f"Sources failed: {stats['sources_failed']}")
        print(f"Processing time: {stats['processing_time_seconds']:.2f}s")
    
    asyncio.run(test_collection())
