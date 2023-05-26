"""
This module helps to restore the backup file present in current working directory /backup
It scans for the recent file and performs restoring of the backup
"""

import pyodbc as odbc
import logging
from my_logging import log_setup
import os
import tomli
from dotenv import load_dotenv

log_setup()  # Initializing logging configurations
logger = logging.getLogger(__name__)

curr_path = os.path.dirname(os.path.realpath(__file__))
db_config_file = os.path.join(curr_path, 'config.toml')
if not os.path.exists(db_config_file):
    logger.error(f'Config file {db_config_file} does not exist')
    exit(1)

# Set the path to the backup file
backup_file_path = os.path.join(curr_path, 'backup')
if not os.path.exists(backup_file_path):
    logger.error(f"Couldn't find backup directory: {backup_file_path}")
    exit(1)

def get_recent_file(directory_path):
    most_recent_file = None
    most_recent_time = 0

    # iterate over the files in the directory using os.scandir
    for entry in os.scandir(directory_path):
        if entry.is_file():
            # get the modification time of the file using entry.stat().st_mtime_ns
            mod_time = entry.stat().st_mtime_ns
            if mod_time > most_recent_time:
                # update the most recent file and its modification time
                most_recent_file = entry.name
                most_recent_time = mod_time
    return most_recent_file

backup_file_name = get_recent_file(backup_file_path)
logger.info(f"[*] Using file {backup_file_name}")
backup_file = os.path.join(backup_file_path, backup_file_name)

curr_os_mssql_files_path = '/var/opt/mssql/data'

logger.debug('Loading database connection configuration...')

with open(db_config_file, 'rb') as config_file:
    config_data: dict = tomli.load(config_file)

# Set the database connection details
DRIVER_NAME = config_data['driver']['name']
SERVER_NAME = config_data['server']['name']
DATABASE_NAME = config_data['database']['name']
USERNAME = config_data['user']['name']
logger.debug('Configurations loaded successfully')

load_dotenv()  # Load the environment containing db password
password = os.getenv('DBpassword')

# Connect to the database
try:
    logger.debug('Connecting to database...')
    conn = odbc.connect(
        f'driver={DRIVER_NAME}',
        host=SERVER_NAME,
        database='master',
        user=USERNAME,
        password=password
    )
    conn.autocommit = True

except Exception as e:
    logger.error(e)
    print('---- Error connecting to database ----')
else:
    logger.info('---- Database connected ! ----')
    # Restore the backup
    cursor = conn.cursor()
    cursor.execute(f"""
    RESTORE FILELISTONLY FROM DISK = N'{backup_file}'
    """)
    
    filelist_dict: dict = {}
    for row in cursor.fetchall():
        filelist_dict[row[2]] = {
            'logical_name': row[0],
            'physical_name': row[1].split('\\')[-1]
        }

    cursor.execute(f"""
        RESTORE DATABASE {DATABASE_NAME} FROM DISK=N'{backup_file}' WITH REPLACE,
        MOVE '{filelist_dict['D']['logical_name']}' TO '{os.path.join(curr_os_mssql_files_path, filelist_dict['D']['physical_name'])}',
        MOVE '{filelist_dict['L']['logical_name']}' TO '{os.path.join(curr_os_mssql_files_path, filelist_dict['L']['physical_name'])}'
        """)
    while cursor.nextset():
        logger.info("Restoring database...")

    logger.info('Database backup restored successfully!')
finally:
    try:
        cursor.close()
        conn.close()
        logger.info('Connection closed successfully')
    except NameError as name_error:
        logger.error(f'Terminating forcefully with error {name_error}')

