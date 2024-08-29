import logging
from typing import Optional

class Logger:
    _instances = {}

    def __new__(cls, name: Optional[str] = None):
        if name not in cls._instances:
            instance = super().__new__(cls)
            instance._logger = logging.getLogger(name or __name__)
            cls._instances[name] = instance
        return cls._instances[name]

    def __init__(self, name: Optional[str] = None, level: int = logging.INFO):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        if not hasattr(self, '_initialized'):
            self.set_level(level)
            self._add_handler()
            self._initialized = True

    def _add_handler(self):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def set_level(self, level: int):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self._logger.setLevel(level)

    def debug(self, message: str):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self._logger.debug(message)

    def info(self, message: str):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self._logger.info(message)

    def warning(self, message: str):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self._logger.warning(message)

    def error(self, message: str):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        self._logger.error(message)

    def critical(self, message: str):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        
        self._logger.critical(message)

class LoggerFactory:
    @staticmethod
    def get_logger(name):
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger