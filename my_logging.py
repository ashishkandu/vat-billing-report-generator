import logging
import logging.config
import yaml
import os

DEFAULT_LEVEL = logging.DEBUG


def log_setup(log_cfg_path: str = 'logger_config.yaml') -> None:
    """
    Initialize custom logging configuration
    """
    if os.path.exists(log_cfg_path):
        with open(log_cfg_path, 'rt') as cfg_file:
            try:
                config = yaml.safe_load(cfg_file.read())
                logging.config.dictConfig(config)
            except yaml.YAMLError as exc:
                print(exc)
            except Exception as exc:
                print('Error loading configuration; Using default configuration')
                logging.basicConfig(level=DEFAULT_LEVEL)
    else:
        logging.basicConfig(level=DEFAULT_LEVEL)
        print('==== Config file for logging not found =====')
        logging.info('Using default configuration')
