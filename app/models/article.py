from sqlalchemy import (
    Column, String, Text, DateTime, Boolean, Integer, 
    ARRAY, Index, Float, JSON
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.ext.mutable import MutableList
from app.models.base import BaseModel
from datetime import datetime
from typing import Dict, List, Optional, Any

class Article(BaseModel):
    """Article model with comprehensive fields for news aggregation"""
    __tablename__ = "articles"
    
    # Unique identifier for content-based deduplication
    content_hash = Column(String(32), unique=True, index=True, nullable=False)
    
    # Core content fields
    title = Column(Text, nullable=False, index=True)
    content = Column(Text)  # Original article content
    summary = Column(Text)  # Basic summary
    url = Column(Text, nullable=False)
    
    # Source information
    source_name = Column(String(100), index=True, nullable=False)
    source_url = Column(Text)  # RSS feed URL
    source_type = Column(String(20), default='rss', index=True)  # rss, api, scrape
    source_reliability = Column(Integer, default=80)  # 0-100 reliability score
    
    # Content classification
    primary_topic = Column(String(50), index=True)  # tech, politics, business, etc.
    secondary_topics = Column(MutableList.as_mutable(ARRAY(String)), default=list)
    importance_level = Column(String(20), default='regular', index=True)  # breaking, important, regular
    
    # Geographic classification
    primary_region = Column(String(50), index=True)  # India, US, Europe, Global
    countries_mentioned = Column(MutableList.as_mutable(ARRAY(String)), default=list)
    language = Column(String(5), default='en', index=True)
    
    # Content metadata
    word_count = Column(Integer, default=0)
    reading_time_minutes = Column(Integer, default=1)  # Estimated reading time
    
    # Timestamps
    published_at = Column(DateTime(timezone=True), index=True)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    processed_at = Column(DateTime(timezone=True))
    
    # Processing status flags
    content_processed = Column(Boolean, default=False)
    hinglish_processed = Column(Boolean, default=False)
    summary_generated = Column(Boolean, default=False)
    ai_processed = Column(Boolean, default=False)
    
    # AI-generated content (when enabled)
    hinglish_content = Column(Text)  # Hinglish version
    ai_summary = Column(Text)  # AI-generated summary
    
    # Financial/stock data (for business news)
    stock_symbols = Column(MutableList.as_mutable(ARRAY(String)), default=list)
    market_sector = Column(String(50))  # Technology, Finance, Healthcare, etc.
    
    # Engagement and quality metrics
    engagement_score = Column(Float, default=0.0)  # Calculated engagement metric
    quality_score = Column(Float, default=0.0)  # Content quality score
    
    # Additional metadata (flexible JSON field for future extensions)
    meta_data = Column(JSONB, default=dict)  # Store any additional data
    
    # Indexes for performance
    __table_args__ = (
        # Composite indexes for common queries
        Index('ix_articles_topic_region', 'primary_topic', 'primary_region'),
        Index('ix_articles_published_desc', 'published_at', postgresql_using='btree', 
              postgresql_ops={'published_at': 'DESC'}),
        Index('ix_articles_importance_published', 'importance_level', 'published_at'),
        Index('ix_articles_source_published', 'source_name', 'published_at'),
        Index('ix_articles_processed_status', 'content_processed', 'ai_processed'),
        
        # GIN indexes for array fields (efficient array searches)
        Index('ix_articles_secondary_topics', 'secondary_topics', postgresql_using='gin'),
        Index('ix_articles_countries', 'countries_mentioned', postgresql_using='gin'),
        Index('ix_articles_stocks', 'stock_symbols', postgresql_using='gin'),
        
        # Partial indexes for common filters
        Index('ix_articles_unprocessed', 'id', postgresql_where='content_processed = false'),
       
    )
    
    def __init__(self, **kwargs):
        # Calculate reading time if word_count is provided
        if 'word_count' in kwargs and kwargs['word_count']:
            kwargs['reading_time_minutes'] = max(1, kwargs['word_count'] // 200)
        
        super().__init__(**kwargs)
    
    def to_dict(self) -> Dict[str, Any]:
        """Enhanced to_dict with proper handling of complex types"""
        data = super().to_dict()
        
        # Convert datetime objects to ISO strings for JSON serialization
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        
        return data
    
    def get_display_content(self, prefer_hinglish: bool = False) -> str:
        """Get content for display, preferring Hinglish if available"""
        if prefer_hinglish and getattr(self, "hinglish_content", None):
            return getattr(self, "hinglish_content")
        return getattr(self, "content", None) or getattr(self, "summary", None) or getattr(self, "title", "")
    
    def get_display_summary(self, prefer_ai: bool = False) -> str:
        """Get summary for display, preferring AI if available"""
        if prefer_ai and getattr(self, "ai_summary", None) is not None:
            return getattr(self, "ai_summary")
        summary = getattr(self, "summary", None)
        content = getattr(self, "content", None)
        title = getattr(self, "title", "")
        if summary is not None:
            return summary
        elif content is not None:
            return content[:200] + "..."
        else:
            return title
    
    def is_recent(self, hours: int = 24) -> bool:
        """Check if article is recent"""
        published_at_value = getattr(self, "published_at", None)
        if not published_at_value:
            return False
        return (datetime.utcnow() - published_at_value).total_seconds() < (hours * 3600)
    
    def is_breaking_news(self) -> bool:
        """Check if this is breaking news"""
        return getattr(self, "importance_level", None) == 'breaking'
    
    def add_topic(self, topic: str):
        """Add a topic to secondary topics if not already present"""
        if self.secondary_topics is None:
            self.secondary_topics = []
        if topic not in self.secondary_topics:
            self.secondary_topics.append(topic)
    
    def add_country(self, country: str):
        """Add a country to countries_mentioned if not already present"""
        if self.countries_mentioned is None:
            self.countries_mentioned = []
        if country not in self.countries_mentioned:
            self.countries_mentioned.append(country)
    
    def mark_processed(self, processing_type: str = 'content'):
        """Mark article as processed"""
        self.processed_at = datetime.utcnow()
        
        if processing_type == 'content':
            self.content_processed = True
        elif processing_type == 'hinglish':
            self.hinglish_processed = True
        elif processing_type == 'summary':
            self.summary_generated = True
        elif processing_type == 'ai':
            self.ai_processed = True
    
    @classmethod
    def get_by_content_hash(cls, db_session, content_hash: str):
        """Get article by content hash"""
        return db_session.query(cls).filter(cls.content_hash == content_hash).first()
    
    @classmethod
    def get_recent_by_topic(cls, db_session, topic: str, hours: int = 24, limit: int = 10):
        """Get recent articles by topic"""
        cutoff_time = func.now() - func.interval(f'{hours} hours')
        return db_session.query(cls).filter(
            cls.primary_topic == topic,
            cls.published_at >= cutoff_time
        ).order_by(cls.published_at.desc()).limit(limit).all()
    
    def __repr__(self):
        return f"<Article(id={self.id}, title='{self.title[:50]}...', source='{self.source_name}')>"
