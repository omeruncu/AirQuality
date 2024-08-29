import functools
from src.utils.logger import LoggerFactory
import time

class ErrorHandler:
    @staticmethod
    def handle_errors(logger_name):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                logger = LoggerFactory.get_logger(logger_name)
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                    raise
            return wrapper
        return decorator

    @staticmethod
    def retry(max_attempts, delay=1):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                logger = LoggerFactory.get_logger(func.__name__)
                for attempt in range(max_attempts):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if attempt < max_attempts - 1:
                            logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay} seconds. Error: {e}")
                            time.sleep(delay)
                        else:
                            logger.error(f"All {max_attempts} attempts failed. Error: {e}")
                            raise
            return wrapper
        return decorator