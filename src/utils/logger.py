import logging

class Logger:
    @staticmethod
    def setup():
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)