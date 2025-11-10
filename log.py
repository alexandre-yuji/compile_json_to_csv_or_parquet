from loguru import logger
from sys import stderr
from functools import wraps

# Removing existing handlers to avoid duplication
logger.remove()

# Logger configuration for stderr
logger.add(
                sink=stderr,
                format="{time} <r>{level}</r> <g>{message}</g> {file}",
                level="INFO"
            )

# Logger configuration for log file
logger.add(
                "my_log_file.log",
                format="{time} {level} {message} {file}",
                level="INFO"
            )

def log_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Calling function '{func.__name__}' with args {args} and kwargs {kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Function '{func.__name__}' returned {result}")
            return result
        except Exception as e:
            logger.exception(f"Exception caught in '{func.__name__}': {e}")
            raise  # Re-throw the exception to not change the behaviour of the decorated function
    return wrapper