import configparser
from pathlib import Path


def get_config_path(filename='dwh.cfg'):
    """Return the absolute Path to the config file."""
    config_path = Path(__file__).resolve().parent.parent / filename
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return config_path


def get_config(filename='dwh.cfg'):
    """Read and return a ConfigParser object from the config file."""
    config_path = get_config_path(filename)

    # prevent lowercasing of keys in config file
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_path)
    return config
