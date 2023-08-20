import pandas as pd

from dbconnection import DBConnection
from filehandlers import TransactionFileHandler

from my_logging import log_setup
import logging
from bs_ad_date_helpers import (get_previous_month_name_np, 
                     get_start_to_end_date_object_in_ad,
                     get_start_to_end_date_object_in_bs,
                     )


log_setup()  # Initializing logging configurations
logger = logging.getLogger(__name__)

def append_transactions_above_1L(transactions: list, PAN_no, name, transaction_type_char, taxable_amount):
    transactions.append([PAN_no, name, 'E', transaction_type_char, taxable_amount, 0])
    

def save_transactions_above_1L(files: dict, transactions):
    df = pd.read_excel(files['1L'])
    new_df = pd.DataFrame(transactions, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    df.to_excel(files['1L'], index=False)
    print(f"\n##### Transactions above 1 Lakh #####\n")
    print(new_df)


def main():

    master_query = """
    SELECT [Transaction Date]
        ,CONCAT_WS('.',[Year], [Month], [Day]) as 'Nepali Date' 
        ,sysTran.[Transaction ID]
        ,[Bill Receiveable Person]
        ,accProfInfo.[Vat Pan No]
        ,STRING_AGG([Inventory Name], '/') as 'Item'
        ,SUM([Item In]) as 'In'
        ,SUM([Item Out]) as 'Out'
        ,amtTran.[Grand Total]
        ,amtTran.[Taxable Amount]
        ,amtTran.[Tax Amount]
        -- ,[Transaction Type]
    FROM [VatBillingSoftware].[dbo].[SystemTransaction] sysTran
    ,[VatBillingSoftware].[dbo].[SystemTransactionPurchaseSalesAmount] amtTran
    ,[VatBillingSoftware].[dbo].[AccountProfileProduct] accProfInfo
    ,[VatBillingSoftware].[dbo].[SystemCalenderDate]
    ,[VatBillingSoftware].[dbo].[SystemTransactionPurchaseSalesItem] psiTran
    ,[VatBillingSoftware].[dbo].[InventoryItem]
    WHERE sysTran.[Transaction Type] = ?
    AND [Transaction Date] BETWEEN ? AND ?
    AND sysTran.[Transaction ID] = amtTran.[Transaction ID]
    AND amtTran.[Account ID] = accProfInfo.[ACCOUNT ID]
    AND [Transaction Date] = [English Date]
    AND [Inventory Item Code] = [Inventory ID]
    AND psiTran.[Transaction ID] = sysTran.[Transaction ID]
    GROUP BY [Transaction Date], [Year], [Month], [Day], sysTran.[Transaction ID]
        ,psiTran.[Transaction ID]
        ,[Bill Receiveable Person]
        ,accProfInfo.[Vat Pan No]
        ,amtTran.[Grand Total]
        ,amtTran.[Taxable Amount]
        ,amtTran.[Tax Amount]
    ORDER BY [Transaction Date]
    """

    with DBConnection('db_config.toml') as db:
        records = db.query(master_query,
                        [transaction_type, START_DATE_AD, END_DATE_AD]
                        )
    extracted_data = [list(row) for row in records]

    headers = ['Date AD', 'Date', 'Transaction ID', 'Bill Receiveable Person', 'PAN No', 'Item', 'Item_in', 'Item_out', 'Total', 'Taxable', 'VAT']
    df = pd.DataFrame(extracted_data, columns=headers)
    df.drop(columns=['Date AD', remove_col], axis=1, inplace=True)
    df.insert(6, 'unit', 'L')
    df.insert(8, 'blank', '')
    df['PAN No'].mask(df['PAN No'] == '', 000, inplace=True)
    df['PAN No'] = df['PAN No'].astype(int)
    df['PAN No'].mask(df['PAN No'] == 000, '', inplace=True)
    df[item_col] = df[item_col].astype(float)
    df['Total'] = df['Total'].astype(float)
    df['Taxable'] = df['Taxable'].astype(float)
    df['VAT'] = df['VAT'].astype(float)
    df.loc['Column_Total']= df.sum(numeric_only=True, axis=0)
    print(f"\n#### {transaction_name.capitalize()} transactions ####\n")
    print(df)

    # Totals
    total_taxable = round(df['Taxable'].iloc[-1], 2)
    print(f'\n[+] {transaction_name.capitalize()} total Taxable: {total_taxable}\n')

    PAN_customers_df = df[df['PAN No'].astype(bool)].copy()
    PAN_customers_df = PAN_customers_df.drop('Column_Total')
    # print(PAN_customers_df.groupby(['PAN No'], as_index=False)['Taxable'].transform('sum'))
    PAN_customers_df = PAN_customers_df.groupby('PAN No').agg({'Bill Receiveable Person': 'first', 'Taxable': 'sum'}).reset_index()

    print(f'\n##### Transactions with PAN No. #####\n')
    print(PAN_customers_df)

    PAN_customers_df_filter = PAN_customers_df[PAN_customers_df['Taxable'].gt(1_00_000)].reset_index()

    transactions_above_1L = []
    for index, row in PAN_customers_df_filter.iterrows():
        append_transactions_above_1L(transactions_above_1L, row['PAN No'], row['Bill Receiveable Person'], trans_char, round(row['Taxable']))

    sheet = files[transaction_name]

    reader = pd.read_excel(sheet)
    with pd.ExcelWriter(
            sheet,
            mode="a",
            engine="openpyxl",
            if_sheet_exists="overlay",
            # engine_kwargs={'options': {'strings_to_numbers': True}},
        ) as writer:
            df.to_excel(writer, index=False, header=False,
                        sheet_name=sheet_name, startrow=len(reader) + 1)
    save_transactions_above_1L(files, transactions_above_1L)
            

if __name__ == '__main__':

    previous_month_np = get_previous_month_name_np()

    START_DATE_AD, END_DATE_AD = get_start_to_end_date_object_in_ad()

    START_DATE_BS, END_DATE_BS = get_start_to_end_date_object_in_bs()

    logger.info(f"#### Fetching transactions for {START_DATE_BS} to {END_DATE_BS} ({previous_month_np}) ####")
    # transaction_type: int = 1

    transactions = (1, 2)

    trans_file = TransactionFileHandler(previous_month_np)
    files = trans_file.files

    for transaction_type in transactions:
        if transaction_type == 2:
            transaction_name = 'sales'
            sheet_name = 'Nepali SB'
            remove_col = 'Item_in'
            item_col = 'Item_out'
            trans_char = 'S'
        elif transaction_type == 1:
            transaction_name = 'purchase'
            sheet_name = 'Nepali PB'
            remove_col = 'Item_out'
            item_col = 'Item_in'
            trans_char = 'P'
        main()