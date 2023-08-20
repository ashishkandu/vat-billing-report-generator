import os
from pyBSDate import addate
from my_logging import log_setup
import logging
import pandas as pd
import shutil
from pathlib import Path

from dbconnection import DBConnection

log_setup()  # Initializing logging configurations
logger = logging.getLogger(__name__)

curr_path = Path.cwd()
sheets_format_directory = curr_path.joinpath('sheets', 'format')

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
        file_name = 'purchase'
        sheet_name = 'Nepali PB'
        break
    elif lookup == 2:
        file_name = 'sales'
        sheet_name = 'Nepali SB'
        break
    else:
        print("! Invalid input")
        continue




from bs_ad_date_helpers import get_previous_month_name_np, get_end_date_of_previous_month, get_start_to_end_date_object_in_ad

end_date_of_previous_month = get_end_date_of_previous_month()

previous_month_np = get_previous_month_name_np()

logger.info(
    f"#### Fetching {file_name} transactions for {end_date_of_previous_month.year} {previous_month_np} ####")

files = {}
for entry in os.scandir(sheets_format_directory):
    if entry.is_file():
        original_name = entry.name
        if file_name in original_name:
            book_name = original_name.split(".")[0].split("-")[0]
            Path().cwd().joinpath('sheets', previous_month_np).mkdir(parents=True, exist_ok=True)
            dest = curr_path.joinpath('sheets', previous_month_np, book_name + " - " +
                                previous_month_np + ".xlsx")
            files[book_name] = dest
            shutil.copyfile(os.path.join(
                sheets_format_directory, original_name), dest)
            
START_DATE, END_DATE = get_start_to_end_date_object_in_ad(
    end_date_of_previous_month)

# START_DATE = datetime.datetime(year=2022, month=7, day=17)

# END_DATE = datetime.datetime(year=2023, month=7, day=16)


with DBConnection('db_config.toml') as db:

    cursor = db.cursor
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
        if row[2] != row[3]:
            print(row[2], row[3])
            print(f'{row[0]:-^20}')
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
        if curr_pan_no == "":
            curr_pan_no = 9999999999
        bs_date = addate(year=row[2].year, month=row[2].month,
                            day=row[2].day).bsdate.strftime("%Y.%m.%d")
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
            if amount+vat != row[4]:
                print('[+] Diff:', row[0], amount+vat, row[4], sep=' | ')
            extracted_data.append((bs_date, row[0], '', row[5], curr_pan_no, 'Diesel/Petrol', round(
                total_litres, 2), 'L', amount+vat, '', amount, vat))
            continue
        amount = inner_rows[0][4]
        vat = inner_rows[0][5]
        if amount+vat != row[4]:
            print('[+] Diff:', row[0], amount+vat, row[4], sep=' | ')
        extracted_data.append((bs_date, row[0], '', row[5], curr_pan_no, inventroy_item_code[inner_rows[0][0]], round(
            inner_rows[0][lookup], 2), 'L', amount+vat, '', amount, vat))
    logger.info("---- Extraction complete ! ----")
    # with open(file_name, 'w') as f:
    #     f.writelines(
    #         [f"{data[0]},{data[1]},{data[2]},{data[3]},{data[4]},{data[5]},{data[6]},{data[7]},{data[8]},{data[9]},{data[10]}\n" for data in extracted_data])
    #     logger.info(f"{file_name} saved successfully")
    sheet = files[file_name]

    reader = pd.read_excel(sheet)
    df = pd.DataFrame(extracted_data)
    print(df)
    df[4] = df[4].astype(int)
    df[6] = df[6].astype(float)
    df[8] = df[8].astype(float)
    df[10] = df[10].astype(float)
    df[11] = df[11].astype(float)

    df[4].mask(df[4] == 9999999999, '', inplace=True)

    if lookup == 2:
        df.drop(df.columns[2], axis=1, inplace=True)

    with pd.ExcelWriter(
        sheet,
        mode="a",
        engine="openpyxl",
        if_sheet_exists="overlay",
        # engine_kwargs={'options': {'strings_to_numbers': True}},
    ) as writer:
        df.to_excel(writer, index=False, header=False,
                    sheet_name=sheet_name, startrow=len(reader) + 1)


