import time
from loguru import logger
from functools import wraps


def time_measure_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Function '{func.__name__}' ran in {end_time - start_time:.4f} seconds")
        return result
    return wrapper