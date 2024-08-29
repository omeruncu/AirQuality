import yaml
import os
from typing import Any, Dict
from src.utils.logger import LoggerFactory

class ConfigManager:
    def __init__(self, config_path='config/config.yaml'):
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.config_path = config_path
        self.config = self._load_config()

    
    def _load_config(self) -> Dict[str, Any]:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        return self._resolve_env_variables(config)

    def _resolve_env_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        for key, value in config.items():
            if isinstance(value, dict):
                config[key] = self._resolve_env_variables(value)
            elif isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                config[key] = os.getenv(env_var)
                if config[key] is None:
                    self.logger.warning(f"Environment variable {env_var} is not set")
        return config

    def get(self, key: str, default: Any = None) -> Any:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    def set(self, key: str, value: Any) -> None:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        keys = key.split('.')
        config = self.config
        for k in keys[:-1]:
            config = config.setdefault(k, {})
        config[keys[-1]] = value

    
    def save(self) -> None:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f)
        self.logger.info(f"Configuration saved to {self.config_path}")

    def reload(self) -> None:
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        
        self.config = self._load_config()
        self.logger.info("Configuration reloaded")

class ConfigManagerFactory:
    @staticmethod
    def create(config_path: str = 'config/config.yaml') -> ConfigManager:
        return ConfigManager(config_path)