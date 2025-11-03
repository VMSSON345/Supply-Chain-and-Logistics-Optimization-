"""
Configuration Loader
"""
import yaml
import json
import os
from typing import Any, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigLoader:
    """Load và quản lý configuration"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.getenv('CONFIG_PATH', 'config/config.yaml')
        self.config = {}
        self.load_config()
    
    def load_config(self):
        """Load configuration từ file"""
        try:
            if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                with open(self.config_path, 'r') as f:
                    self.config = yaml.safe_load(f)
            elif self.config_path.endswith('.json'):
                with open(self.config_path, 'r') as f:
                    self.config = json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {self.config_path}")
            
            logger.info(f"✅ Configuration loaded from {self.config_path}")
            
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}. Using defaults.")
            self.load_defaults()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            self.load_defaults()
    
    def load_defaults(self):
        """Load default configuration"""
        self.config = {
            'spark': {
                'app_name': 'RetailAnalytics',
                'master': 'spark://spark-master:7077',
                'driver_memory': '4g',
                'executor_memory': '4g',
                'executor_cores': 4
            },
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topic': 'retail_transactions',
                'group_id': 'retail-consumer-group'
            },
            'elasticsearch': {
                'host': 'http://localhost:9200',
                'timeout': 30,
                'max_retries': 3
            },
            'batch_processing': {
                'association_rules': {
                    'min_support': 0.01,
                    'min_confidence': 0.3
                },
                'forecasting': {
                    'periods': 30,
                    'top_n_products': 50
                },
                'segmentation': {
                    'n_clusters': 5
                }
            },
            'streaming': {
                'window_duration': '5 minutes',
                'watermark': '10 minutes',
                'checkpoint_location': '/opt/spark-data/checkpoints'
            },
            'alerts': {
                'high_demand_threshold': 2.5,
                'low_stock_threshold': 0.3,
                'price_spike_threshold': 1.5
            }
        }
        logger.info("✅ Default configuration loaded")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports nested keys with dot notation)
        
        Example: config.get('spark.driver_memory')
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def to_dict(self) -> Dict:
        """Return configuration as dictionary"""
        return self.config
    
    def save(self, output_path: str = None):
        """Save configuration to file"""
        output_path = output_path or self.config_path
        
        with open(output_path, 'w') as f:
            if output_path.endswith('.yaml') or output_path.endswith('.yml'):
                yaml.dump(self.config, f, default_flow_style=False)
            elif output_path.endswith('.json'):
                json.dump(self.config, f, indent=2)
        
        logger.info(f"✅ Configuration saved to {output_path}")


# Singleton instance
_config_instance = None

def get_config(config_path: str = None) -> ConfigLoader:
    """Get singleton config instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigLoader(config_path)
    return _config_instance
