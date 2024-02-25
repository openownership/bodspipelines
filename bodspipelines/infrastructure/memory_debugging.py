import tracemalloc
import os, psutil
from loguru import logger

# Configure log file
logger.add("memory.log")

# Start tracing
tracemalloc.start()

def log_memory():
    """Log current memory usage and largest objects"""
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    logger.info(f"Process size: {psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2} MB")
    for stat in top_stats[:10]:
        logger.info(stat)
