import pyodbc as odbc
from dotenv import load_dotenv
import os
import nepali_datetime
import datetime
from pyBSDate import bsdate, addate
import tomli
from my_logging import log_setup
import logging

log_setup()  # Initializing logging configurations
logger = logging.getLogger(__name__)

curr_path = os.path.dirname(os.path.realpath(__file__))
db_config_file = os.path.join(curr_path, 'config.toml')

logger.debug('Loading database connection configuration...')

print("""### Report Generator ###
1. Purchase
2. Sales
""")
while True:
    try:
        lookup = int(input('Enter 1 or 2: '))
    except ValueError as ve:
        print("! Please enter specified values only...")
    if lookup == 1:
        file_name = 'purchase.csv'
        break
    elif lookup == 2:
        file_name = 'sales.csv'
        break
    else:
        print("! Invalid input")
        continue

if not os.path.exists(db_config_file):
    logger.error(f'Config file {db_config_file} does not exist')
    exit(1)

with open(db_config_file, 'rb') as config_file:
    config_data: dict = tomli.load(config_file)

DRIVER_NAME = config_data['driver']['name']
SERVER_NAME = config_data['server']['name']
DATABASE_NAME = config_data['database']['name']
USERNAME = config_data['user']['name']
logger.debug('Configurations loaded successfully')

def get_last_date_of_previous_month(current_year: int, current_month: int):
    """ 
    Retruns the previous month's last date in B.S. in the format of 2079-12-30 
    Day is hardcoded to first day of passed current month.
    """
    previous_month_date = nepali_datetime.date(
        current_year, current_month, 1) - datetime.timedelta(days=1)
    return previous_month_date


def get_start_to_end_date_object_in_ad(any_date):
    """
    Returns tuple of (start_date: datetime.date, end_date: datetime.date) in AD
    """
    ne_date_start = bsdate(year=any_date.year, month=any_date.month, day=1)
    ne_date_end = bsdate(year=any_date.year,
                         month=any_date.month, day=any_date.day)
    en_date_start = ne_date_start.addate
    en_date_end = ne_date_end.addate
    return datetime.date(en_date_start.year, en_date_start.month, en_date_start.day), datetime.date(en_date_end.year, en_date_end.month, en_date_end.day)


today = nepali_datetime.date.today()
date_of_previous_month = get_last_date_of_previous_month(
    today.year, today.month)
logger.info(
    f"#### Fetching transactions for {date_of_previous_month.year} {nepali_datetime._FULLMONTHNAMES[date_of_previous_month.month]} ####")

START_DATE, END_DATE = get_start_to_end_date_object_in_ad(
    date_of_previous_month)

load_dotenv()  # Load the environment containing db password
password = os.getenv('DBpassword')

try:
    logger.debug('Connecting to database...')
    conn = odbc.connect(
        f'driver={DRIVER_NAME}',
        host=SERVER_NAME,
        database=DATABASE_NAME,
        user=USERNAME,
        password=password
    )

except Exception as e:
    logger.error(e)
    print('---- Error connecting to database ----')
else:
    logger.info('---- Database connected ! ----')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT [Transaction ID], [Transaction Type], [Transaction Date], [Bill Date], [Transaction Amount], [Bill Receiveable Person]
        FROM VatBillingSoftware.dbo.SystemTransaction
        WHERE [Transaction Date] >= ? AND [Transaction Date] <= ? AND [Transaction Type] = ?
        """,
                   [START_DATE, END_DATE, lookup]
                   )
    rows = cursor.fetchall()
    logger.debug('SystemTransaction fetch complete')

    extracted_data = []

    inventroy_item_code = {
        '01-0001': 'Petrol',
        '01-0002': 'Diesel'
    }

    for row in rows:
        # print(row[0], row[5], row[2])
        if row[2] != row[3]: print(f'{row[0]: -^20}')
        cursor.execute("""
            SELECT [Inventory Item Code], [Item In], [Item Out], [ACCOUNT ID], [VATABLE AMOUNT], [VAT AMOUNT]
            FROM VatBillingSoftware.dbo.SystemTransactionPurchaseSalesItem
            WHERE [Transaction ID] = ?
        """,
                       [row[0]]

                       )
        inner_rows = cursor.fetchall()
        curr_pan_no = cursor.execute("""
        SELECT [Vat Pan No]
        FROM VatBillingSoftware.dbo.AccountProfileProduct
        WHERE [ACCOUNT ID] = ?
        """, inner_rows[0][3]).fetchval()
        bs_date = addate(year=row[2].year, month=row[2].month, day=row[2].day).bsdate.strftime("%Y.%m.%d")
        # formatted_bs_date = bs_date.strftime("%Y.%m.%d")
        # print(formatted_bs_date)

        if len(inner_rows) > 1:
            total_litres = 0
            amount = 0
            vat = 0
            for inner_row in inner_rows:
                total_litres += inner_row[lookup]
                amount += inner_row[4]
                vat += inner_row[5]
            if amount+vat != row[4]: print('[+] Diff:', row[0], amount+vat, row[4], sep=' | ')
            extracted_data.append((bs_date, row[0], row[5], curr_pan_no, 'Diesel/Petrol', round(total_litres, 2), 'L', amount+vat, '', amount, vat))
            continue
        amount = inner_rows[0][4]
        vat = inner_rows[0][5]
        if amount+vat != row[4]: print('[+] Diff:', row[0], amount+vat, row[4], sep=' | ')
        extracted_data.append((bs_date, row[0], row[5], curr_pan_no, inventroy_item_code[inner_rows[0][0]], round(inner_rows[0][lookup], 2), 'L', amount+vat, '', amount, vat))
    logger.info("---- Extraction complete ! ----")
    with open(file_name, 'w') as f:
        f.writelines(
            [f"{data[0]},{data[1]},{data[2]},{data[3]},{data[4]},{data[5]},{data[6]},{data[7]},{data[8]},{data[9]},{data[10]}\n" for data in extracted_data])
        logger.info(f"{file_name} saved successfully")

finally:
    try:
        cursor.close()
        conn.close()
    except NameError as name_error:
        logger.error(f'Terminating forcefully with error {name_error}')
