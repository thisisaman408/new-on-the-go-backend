"""
Cloud-ready Celery application with Redis backend
Handles background tasks for RSS collection, content processing, and deduplication
"""

import os
from datetime import timedelta, datetime
from celery import Celery
from celery.schedules import crontab
from kombu import Queue
import logging

from app.config import settings

logger = logging.getLogger(__name__)

def create_celery_app() -> Celery:
    """Create and configure Celery application with cloud-ready settings"""
    
    # Create Celery instance
    celery_app = Celery('news_aggregator')
    
    # Detect environment and configure broker
    broker_url = _get_broker_url()
    result_backend = _get_result_backend()
    
    # Core Celery configuration
    celery_app.conf.update(
        # Broker settings
        broker_url=broker_url,
        result_backend=result_backend,
        
        # Task serialization
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        
        # Task routing and queues
        task_routes={
            'app.tasks.rss_tasks.collect_all_rss_sources': {'queue': 'rss_collection'},
            'app.tasks.rss_tasks.collect_single_source': {'queue': 'rss_sources'},
            'app.tasks.rss_tasks.process_articles_background': {'queue': 'content_processing'},
            'app.tasks.rss_tasks.deduplicate_articles_background': {'queue': 'maintenance'},
        },
        
        # Define queues
        task_default_queue='default',
        task_queues=(
            Queue('default', routing_key='default'),
            Queue('rss_collection', routing_key='rss_collection'),
            Queue('rss_sources', routing_key='rss_sources'),
            Queue('content_processing', routing_key='content_processing'),
            Queue('maintenance', routing_key='maintenance'),
        ),
        
        # Worker settings
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        worker_max_tasks_per_child=1000,
        
        # Task execution settings
        task_soft_time_limit=300,  # 5 minutes
        task_time_limit=600,       # 10 minutes
        task_reject_on_worker_lost=True,
        
        # Retry settings
        task_default_retry_delay=60,
        task_max_retries=3,
        
        # Result settings
        result_expires=3600,  # 1 hour
        task_ignore_result=False,
        
        # Beat schedule for periodic tasks
        beat_schedule={
            'collect-rss-every-15-minutes': {
                'task': 'app.tasks.rss_tasks.collect_all_rss_sources',
                'schedule': timedelta(minutes=15),
                'options': {'queue': 'rss_collection'}
            },
            'process-content-every-30-minutes': {
                'task': 'app.tasks.rss_tasks.process_articles_background',
                'schedule': timedelta(minutes=30),
                'options': {'queue': 'content_processing'}
            },
            'deduplicate-daily': {
                'task': 'app.tasks.rss_tasks.deduplicate_articles_background',
                'schedule': crontab(hour='2', minute='0'),  # 2 AM UTC daily
                'options': {'queue': 'maintenance'}
            },
            'health-check-hourly': {
                'task': 'app.tasks.rss_tasks.health_check_sources',
                'schedule': crontab(minute='0'),  # Every hour
                'options': {'queue': 'maintenance'}
            }
        },
        
        # Monitoring and logging
        worker_send_task_events=True,
        task_send_sent_event=True,
        
        # Connection settings for cloud deployment
        broker_connection_retry_on_startup=True,
        broker_connection_max_retries=10,
        broker_transport_options={
            'visibility_timeout': 3600,
            'fanout_prefix': True,
            'fanout_patterns': True
        }
    )
    
    # Configure logging
    _configure_celery_logging(celery_app)
    
    # Auto-discover tasks
    celery_app.autodiscover_tasks(['app.tasks'])
    
    logger.info(f"Celery app configured with broker: {_mask_credentials(broker_url)}")
    return celery_app

def _get_broker_url() -> str:
    """Get broker URL with environment detection"""
    # Check for cloud Redis first (environment variables)
    cloud_redis = os.getenv('REDIS_URL') or os.getenv('REDISCLOUD_URL')
    if cloud_redis:
        logger.info("Using cloud Redis broker")
        return cloud_redis
    
    # Fallback to local configuration
    logger.info("Using local Redis broker")
    return settings.CELERY_BROKER_URL

def _get_result_backend() -> str:
    """Get result backend URL with environment detection"""
    # Use same logic as broker
    cloud_redis = os.getenv('REDIS_URL') or os.getenv('REDISCLOUD_URL')
    if cloud_redis:
        return cloud_redis
    
    return settings.CELERY_RESULT_BACKEND

def _configure_celery_logging(celery_app: Celery):
    """Configure Celery logging for better monitoring"""
    from celery.signals import setup_logging
    
    @setup_logging.connect
    def config_loggers(*args, **kwargs):
        from logging.config import dictConfig
        
        dictConfig({
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'default': {
                    'format': '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
                },
            },
            'handlers': {
                'console': {
                    'level': 'INFO',
                    'class': 'logging.StreamHandler',
                    'formatter': 'default',
                },
            },
            'root': {
                'level': 'INFO',
                'handlers': ['console'],
            },
            'loggers': {
                'celery': {
                    'level': 'INFO',
                    'handlers': ['console'],
                    'propagate': False,
                },
            }
        })

def _mask_credentials(url: str) -> str:
    """Mask credentials in URL for logging"""
    import re
    return re.sub(r'://([^:]+):([^@]+)@', '://***:***@', url)

# Create global Celery instance
celery_app = create_celery_app()

# Health check task
@celery_app.task(bind=True)
def health_check(self):
    """Basic health check task"""
    return {
        'status': 'healthy',
        'worker_id': self.request.id,
        'timestamp': str(datetime.utcnow())
    }

# For testing
if __name__ == '__main__':
    print("Testing Celery configuration...")
    print(f"Broker: {_mask_credentials(celery_app.conf.broker_url)}")
    print(f"Backend: {_mask_credentials(celery_app.conf.result_backend)}")
    print(f"Queues: {[q.name for q in celery_app.conf.task_queues]}")
    print("✅ Celery app configured successfully!")
