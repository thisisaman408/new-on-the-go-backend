from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import settings
import logging
from sqlalchemy import text
logger = logging.getLogger(__name__)

# Create both sync and async engines for flexibility
def create_database_engines():
    """Create database engines with cloud migration support"""
    
    # Parse database URL for different environments
    db_url = settings.DATABASE_URL
    
    # For local development with PostgreSQL
    if db_url.startswith("postgresql://"):
        # Sync engine for migrations and admin tasks
        sync_engine = create_engine(
            db_url,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=300,    # Recycle connections every 5 minutes
            echo=settings.DEBUG  # Log SQL queries in debug mode
        )
        
        # Async engine for main application (better performance)
        async_db_url = db_url.replace("postgresql://", "postgresql+asyncpg://")
        async_engine = create_async_engine(
            async_db_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=settings.DEBUG
        )
        
    # For cloud databases (Supabase, Railway, etc.) - same pattern works
    elif db_url.startswith("postgres://"):
        # Convert postgres:// to postgresql:// (some cloud providers use postgres://)
        corrected_url = db_url.replace("postgres://", "postgresql://")
        
        sync_engine = create_engine(
            corrected_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=settings.DEBUG
        )
        
        async_db_url = corrected_url.replace("postgresql://", "postgresql+asyncpg://")
        async_engine = create_async_engine(
            async_db_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=settings.DEBUG
        )
    
    else:
        raise ValueError(f"Unsupported database URL format: {db_url}")
    
    return sync_engine, async_engine

# Initialize engines
sync_engine, async_engine = create_database_engines()

# Session makers
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Base class for all models
Base = declarative_base()

# Database dependency for FastAPI
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            await session.close()

# Sync database session (for migrations and scripts)
def get_sync_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        logger.error(f"Sync database session error: {e}")
        raise
    finally:
        db.close()

# Database health check
async def check_database_connection():
    """Check if database is accessible"""
    try:
        async with AsyncSessionLocal() as session:
           
            result = await session.execute(text("SELECT 1"))
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

# Cloud migration helper
def get_database_info():
    """Get database info for migration planning"""
    return {
        "engine_type": "postgresql",
        "supports_async": True,
        "supports_json": True,
        "supports_arrays": True,
        "supports_vector": True,  # pgvector extension
        "current_url": settings.DATABASE_URL,
        "migration_ready": True
    }
