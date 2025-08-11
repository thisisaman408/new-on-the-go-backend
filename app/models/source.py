from sqlalchemy import (
    Column, String, Text, Boolean, Integer, 
    DateTime, Float, ARRAY, JSON
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.models.base import BaseModel
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
class NewsSource(BaseModel):
    """RSS/API news source model"""
    __tablename__ = "news_sources"
    
    # Source identification
    name = Column(String(100), nullable=False, index=True)
    url = Column(Text, nullable=False, unique=True)  # RSS feed URL or API endpoint
    source_type = Column(String(20), default='rss', index=True)  # rss, api, scrape
    
    # Geographic and topic classification
    primary_region = Column(String(50), index=True)  # India, US, Europe, Global
    country_code = Column(String(3), index=True)  # ISO country code (IN, US, GB)
    topics = Column(ARRAY(String), default=list)  # [tech, politics, business]
    language = Column(String(5), default='en', index=True)
    
    # Source quality and reliability
    reliability_score = Column(Integer, default=80, index=True)  # 0-100 score
    quality_rating = Column(String(10), default='good')  # excellent, good, fair, poor
    
    # Polling configuration
    poll_frequency_minutes = Column(Integer, default=15)  # How often to poll
    max_articles_per_poll = Column(Integer, default=20)  # Limit articles per fetch
    enabled = Column(Boolean, default=True, index=True)
    
    # Status tracking
    last_poll_at = Column(DateTime(timezone=True))
    last_successful_poll_at = Column(DateTime(timezone=True))
    next_poll_at = Column(DateTime(timezone=True))
    
    # HTTP caching headers (for RSS feeds)
    last_etag = Column(String(100))  # ETag header for conditional requests
    last_modified = Column(String(100))  # Last-Modified header
    
    # Performance metrics
    total_polls = Column(Integer, default=0)
    successful_polls = Column(Integer, default=0)
    failed_polls = Column(Integer, default=0)
    total_articles_collected = Column(Integer, default=0)
    
    # Response time tracking
    avg_response_time_ms = Column(Float, default=0.0)
    last_response_time_ms = Column(Float, default=0.0)
    
    # Error tracking
    consecutive_failures = Column(Integer, default=0)
    last_error_message = Column(Text)
    last_error_at = Column(DateTime(timezone=True))
    
    # Configuration and metadata
    custom_headers = Column(JSONB, default=dict)  # Custom HTTP headers
    parsing_config = Column(JSONB, default=dict)  # Source-specific parsing rules
    meta_data = Column(JSONB, default=dict)  # Additional flexible data
    
    def __init__(self, **kwargs):
        # Set next poll time based on frequency
        if 'poll_frequency_minutes' in kwargs:
            kwargs['next_poll_at'] = datetime.utcnow() + timedelta(
                minutes=kwargs['poll_frequency_minutes']
            )
        
        super().__init__(**kwargs)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        total_polls = getattr(self, 'total_polls', 0)
        successful_polls = getattr(self, 'successful_polls', 0)
        if not total_polls or total_polls == 0:
            return 0.0
        return (successful_polls / total_polls) * 100
    
    @property
    def is_healthy(self) -> bool:
        return (
            bool(self.enabled) and
            # Ignore the type error: we know this runs on a loaded instance,
            # so `self.consecutive_failures` will be a real integer.
            int(self.consecutive_failures) < 5 and  # type: ignore[arg-type]
            self.success_rate > 70.0
        )
        
    @property
    def is_due_for_poll(self) -> bool:
        """Check if source is due for polling"""
        if self.enabled is not True or self.next_poll_at is None:
            return False
        # Use the recommended timezone-aware `now()` method
        return bool(datetime.now(timezone.utc) >= self.next_poll_at)
    
    def record_successful_poll(self, response_time_ms: float, articles_count: int):
        """Record a successful poll"""
        now = datetime.utcnow()
        
        self.last_poll_at = now
        self.last_successful_poll_at = now
        self.next_poll_at = now + timedelta(minutes=getattr(self, 'poll_frequency_minutes', 15))
        
        self.total_polls += 1
        self.successful_polls += 1
        self.consecutive_failures = 0
        self.total_articles_collected += articles_count
        
        # Update response time (moving average)
        avg_response_time = getattr(self, 'avg_response_time_ms', 0.0)
        if avg_response_time == 0.0:
            self.avg_response_time_ms = response_time_ms
        else:
            self.avg_response_time_ms = (float(avg_response_time) * 0.8) + (response_time_ms * 0.2)
        
        self.last_response_time_ms = response_time_ms
        
        # Improve reliability score for consistent success
        if int(getattr(self, 'consecutive_failures', 0)) == 0 and getattr(self, 'reliability_score', 0) < 95:
            self.reliability_score = min(95, getattr(self, 'reliability_score', 0) + 1)
    
    def record_failed_poll(self, error_message: str):
        """Record a failed poll"""
        now = datetime.utcnow()
        
        self.last_poll_at = now
        self.last_error_at = now
        self.last_error_message = error_message[:500]  # Limit error message length
        
        self.total_polls += 1
        self.failed_polls += 1
        self.consecutive_failures += 1
        
        # Reduce reliability score for failures
        if int(getattr(self, 'reliability_score', 0)) > 20:
            self.reliability_score = max(20, int(getattr(self, 'reliability_score', 0)) - 2)
        
        # Increase poll frequency for failed sources (backoff)
        poll_frequency = getattr(self, 'poll_frequency_minutes', 15)
        consecutive_failures = getattr(self, 'consecutive_failures', 0)
        backoff_minutes = min(60, int(poll_frequency) + (int(consecutive_failures) * 5))
        self.next_poll_at = now + timedelta(minutes=backoff_minutes)
        
        # Disable source after too many consecutive failures
        if int(getattr(self, 'consecutive_failures', 0)) >= 10:
            self.enabled = False
    
    def update_caching_headers(self, etag: Optional[str] = None, last_modified: Optional[str] = None):
        """Update HTTP caching headers"""
        if etag:
            self.last_etag = etag
        if last_modified:
            self.last_modified = last_modified
    
    def get_custom_header(self, header_name: str) -> Optional[str]:
        """Get custom header value"""
        if isinstance(self.custom_headers, dict):
            return self.custom_headers.get(header_name)
        return None
    
    def set_custom_header(self, header_name: str, header_value: str):
        """Set custom header"""
        if not isinstance(self.custom_headers, dict) or self.custom_headers is None:
            self.custom_headers = {}
        self.custom_headers[header_name] = header_value
    
    def add_topic(self, topic: str):
        """Add a topic if not already present"""
        if self.topics is None:
            self.topics = []
        if topic not in self.topics:
            self.topics.append(topic)
    
    def remove_topic(self, topic: str):
        """Remove a topic"""
        if isinstance(self.topics, list) and topic in self.topics:
            self.topics.remove(topic)
    
    @classmethod
    def get_enabled_sources(cls, db_session):
        """Get all enabled sources"""
        return db_session.query(cls).filter(cls.enabled == True).all()
    
    @classmethod
    def get_sources_due_for_poll(cls, db_session):
        """Get sources that are due for polling"""
        now = datetime.utcnow()
        return db_session.query(cls).filter(
            cls.enabled == True,
            cls.next_poll_at <= now
        ).all()
    
    @classmethod
    def get_by_region(cls, db_session, region: str):
        """Get sources by region"""
        return db_session.query(cls).filter(
            cls.primary_region == region,
            cls.enabled == True
        ).all()
    
    @classmethod
    def get_by_topic(cls, db_session, topic: str):
        """Get sources that cover a specific topic"""
        return db_session.query(cls).filter(
            cls.topics.any(topic), # type: ignore[attr-defined]
            cls.enabled == True
        ).all()
    
    def to_dict(self) -> Dict[str, Any]:
        """Enhanced to_dict with computed properties"""
        data = super().to_dict()
        data.update({
            'success_rate': self.success_rate,
            'is_healthy': self.is_healthy,
            'is_due_for_poll': self.is_due_for_poll
        })
        
        # Convert datetime objects to ISO strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        
        return data
    
    def __repr__(self):
        return f"<NewsSource(id={self.id}, name='{self.name}', region='{self.primary_region}', enabled={self.enabled})>"
