"""
Tasks module initialization
Exports Celery app and task functions for easy importing
"""

from app.tasks.celery_app import celery_app
from app.tasks.rss_tasks import (
    collect_all_rss_sources,
    collect_single_source,
    process_articles_background,
    deduplicate_articles_background,
    health_check_sources,
    manual_source_trigger,
    get_task_status,
    get_active_tasks
)

__all__ = [
    'celery_app',
    'collect_all_rss_sources',
    'collect_single_source',
    'process_articles_background',
    'deduplicate_articles_background',
    'health_check_sources',
    'manual_source_trigger',
    'get_task_status',
    'get_active_tasks'
]

# Task registry for easy access
TASK_REGISTRY = {
    'rss.collect_all': 'app.tasks.rss_tasks.collect_all_rss_sources',
    'rss.collect_single': 'app.tasks.rss_tasks.collect_single_source',
    'content.process': 'app.tasks.rss_tasks.process_articles_background',
    'maintenance.deduplicate': 'app.tasks.rss_tasks.deduplicate_articles_background',
    'monitoring.health_check': 'app.tasks.rss_tasks.health_check_sources',
    'manual.trigger': 'app.tasks.rss_tasks.manual_source_trigger'
}

def get_task_by_name(task_name: str):
    """Get task function by registry name"""
    if task_name in TASK_REGISTRY:
        return celery_app.tasks[TASK_REGISTRY[task_name]]
    return None
