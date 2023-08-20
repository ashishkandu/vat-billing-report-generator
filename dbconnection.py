import pyodbc as odbc
from dotenv import load_dotenv
from pathlib import Path
import tomli
import os
from dotenv import load_dotenv

from my_logging import log_setup
import logging

log_setup()  # Initializing logging configurations
logger = logging.getLogger(__name__)

class DBConnection:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(DBConnection)
        return cls._instance
    
    def __init__(self, db_config_filename):
        self.loadDBConfig(db_config_filename)
        try:
            logger.debug('Connecting to database...')
            self._conn = odbc.connect(
                f'driver={self.DRIVER_NAME}',
                host=self.SERVER_NAME,
                database=self.DATABASE_NAME,
                user=self.USERNAME,
                password=self.password
            )
            self._cursor = self._conn.cursor()
            logger.info('---- Database connected ! ----')

        except Exception as e:
            logger.error('---- Error connecting to database ----')
            logger.exception(e)
            raise SystemExit(1)
    

    def loadDBConfig(self, db_config_filename):
        db_config_file = Path().cwd().joinpath(db_config_filename)
        if not db_config_file.exists():
            logger.error(f'Config file {db_config_file} does not exist')
            exit(1)

        with open(db_config_file, 'rb') as config_file:
            config_data: dict = tomli.load(config_file)

        self.DRIVER_NAME = config_data['driver']['name']
        self.SERVER_NAME = config_data['server']['name']
        self.DATABASE_NAME = config_data['database']['name']
        self.USERNAME = config_data['user']['name']
        load_dotenv()  # Load the environment containing db password
        self.password = os.getenv('DBpassword')
        logger.debug('DB configurations loaded successfully')
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.connection.close()

    @property
    def connection(self):
        return self._conn
    
    @property
    def cursor(self):
        return self._cursor
    
    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()