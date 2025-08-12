from pydantic_settings import BaseSettings
from typing import List, Optional, Literal
import os

class Settings(BaseSettings):
    # Database Configuration
    DATABASE_URL: str = "postgresql://newsuser:newspass@localhost:5432/newsdb"
    
    # Redis Configuration
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CACHE_DB: int = 1
    CELERY_CUSTOM_WORKER_POOL: Optional[str] = None
    # Celery Configuration
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    
    # AI Configuration (Optional - can be enabled/disabled)
    AI_ENABLED: bool = False  # Master AI switch
    AI_PROCESSING_ENABLED: bool = False
    AI_PROVIDER: Literal["openai", "perplexity", "hybrid"] = "openai"
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_MODEL: str = "gpt-3.5-turbo"  # Cost-effective model
    AI_MAX_REQUESTS_PER_MINUTE: int = 30  # Rate limiting
    AI_PROCESSING_ENABLED: bool = False  # Separate processing toggle
    
    # Application Settings
    APP_NAME: str = "News Aggregator"
    DEBUG: bool = True  # Set to False in production
    API_V1_STR: str = "/api/v1"
    
    # RSS Collection Settings
    RSS_POLL_INTERVAL: int = 15  # minutes
    MAX_ARTICLES_PER_FEED: int = 20
    CONTENT_CACHE_TTL: int = 86400  # 24 hours in seconds
    
    # Content Processing (Works without AI)
    ENABLE_CONTENT_ENHANCEMENT: bool = True  # Rule-based improvements
    ENABLE_TOPIC_CLASSIFICATION: bool = True  # Keyword-based classification
    ENABLE_SUMMARIZATION: bool = True  # Basic text summarization
    
    # Rate Limiting
    RSS_CONCURRENT_REQUESTS: int = 10
    
    # Security Settings
    SECRET_KEY: str = "your-secret-key-change-this-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = True

    def is_ai_available(self) -> bool:
        """Check if AI processing is fully available"""
        return (
            self.AI_ENABLED and 
            self.OPENAI_API_KEY is not None and 
            self.AI_PROCESSING_ENABLED
        )
    
    @property
    def database_url_sync(self) -> str:
        """Get synchronous database URL for migrations"""
        return self.DATABASE_URL
    
    @property
    def database_url_async(self) -> str:
        """Get asynchronous database URL for main app"""
        if self.DATABASE_URL.startswith("postgresql://"):
            return self.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
        elif self.DATABASE_URL.startswith("postgres://"):
            return self.DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")
        return self.DATABASE_URL

# Global settings instance
settings = Settings()
