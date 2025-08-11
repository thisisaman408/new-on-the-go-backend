#!/usr/bin/env python3
"""Database initialization script"""

import asyncio
import sys
import os
from sqlalchemy import text
# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import async_engine, check_database_connection
from app.models.base import Base
from app.models.article import Article
from app.models.source import NewsSource
from app.config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_tables():
    """Create all tables"""
    logger.info("Creating database tables...")
    
    try:
        async with async_engine.begin() as conn:
            # Drop all tables (for fresh start in development)
            if settings.DEBUG:
                logger.warning("DEBUG mode: Dropping existing tables")
                await conn.run_sync(Base.metadata.drop_all)
            
            # Create all tables
            await conn.run_sync(Base.metadata.create_all)
            
        logger.info("Database tables created successfully!")
        
        # Check connection
        if await check_database_connection():
            logger.info("Database connection verified!")
        else:
            logger.error("Database connection failed!")
            
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise

async def create_extensions():
    """Create PostgreSQL extensions"""
    logger.info("Creating PostgreSQL extensions...")
    
    try:
        async with async_engine.begin() as conn:
            # Create pgvector extension for future vector similarity search
         
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            # Create pg_stat_statements for query performance monitoring
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"))
            
        logger.info("PostgreSQL extensions created successfully!")
        
    except Exception as e:
        logger.warning(f"Extension creation failed (this is okay for local dev): {e}")

def print_database_info():
    """Print database information"""
    from app.database import get_database_info
    
    info = get_database_info()
    logger.info("Database Configuration:")
    for key, value in info.items():
        logger.info(f"  {key}: {value}")

async def main():
    """Main initialization function"""
    logger.info("Initializing database...")
    
    print_database_info()
    
    try:
        await create_extensions()
        await create_tables()
        
        logger.info("✅ Database initialization completed successfully!")
        logger.info("Next steps:")
        logger.info("1. Run: python scripts/seed_sources.py")
        logger.info("2. Start the application: uvicorn app.main:app --reload")
        
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
