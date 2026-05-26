import os
import yaml

class Config:
    _config = None

    @classmethod
    def load(cls):
        if cls._config is not None:
            return cls._config

        # Look for config.yaml in the project root
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, 'config.yaml')
        
        if not os.path.exists(config_path):
            # Fallback to reporter directory (for backward compatibility if needed)
            config_path = os.path.join(base_dir, 'reporter', 'config.yaml')
        
        if not os.path.exists(config_path):
            cls._config = {}
            return cls._config
        
        try:
            with open(config_path, 'r') as f:
                cls._config = yaml.safe_load(f) or {}
        except Exception:
            cls._config = {}
            
        return cls._config

    @classmethod
    def get(cls, key_path, default=None):
        config = cls.load()
        keys = key_path.split('.')
        value = config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    @classmethod
    def set(cls, key_path, value):
        config = cls.load()
        keys = key_path.split('.')
        curr = config
        for i, key in enumerate(keys[:-1]):
            if key not in curr or not isinstance(curr[key], dict):
                curr[key] = {}
            curr = curr[key]
        curr[keys[-1]] = value
        return config

    @classmethod
    def save(cls):
        if cls._config is None:
            return
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, 'config.yaml')
        
        try:
            with open(config_path, 'w') as f:
                yaml.safe_dump(cls._config, f, default_flow_style=False, sort_keys=False)
            return True
        except Exception:
            return False

def get_config(key_path, default=None):
    return Config.get(key_path, default)

def set_config(key_path, value, save=True):
    Config.set(key_path, value)
    if save:
        Config.save()
