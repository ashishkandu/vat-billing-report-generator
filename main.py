import pandas as pd
from dataclasses import dataclass, fields
from typing import List, Any

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

@dataclass
class Transaction:
    transaction_type: int
    transaction_name: str
    sheet_name: str
    remove_col: str
    item_col: str
    trans_char: str
    records: List[List[Any]]

@dataclass
class Transactions:
    sales: Transaction
    purchase: Transaction

    def __iter__(self):
        for field in fields(self):
            yield getattr(self, field.name)

def append_transactions_above_1L(transactions: list, PAN_no, name, transaction_type_char, taxable_amount):
    transactions.append([PAN_no, name, 'E', transaction_type_char, taxable_amount, 0])
    

def save_transactions_above_1L(files: dict, transactions):
    df = pd.read_excel(files['1L'])
    new_df = pd.DataFrame(transactions, columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    df.to_excel(files['1L'], index=False)
    print(f"\n##### Transactions above 1 Lakh #####\n")
    print(new_df)


def main(transactions: Transactions):

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
        purchase_records = db.query(master_query,
                        [transactions.purchase.transaction_type, START_DATE_AD, END_DATE_AD]
                        )
        sales_records = db.query(master_query,
                        [transactions.sales.transaction_type, START_DATE_AD, END_DATE_AD]
                        )
    
    transactions.purchase.records = [list(row) for row in purchase_records] 
    transactions.sales.records = [list(row) for row in sales_records] 

    transactions_above_1L = []

    transaction: Transaction
    for transaction in transactions:
        headers = ['Date AD', 'Date', 'Transaction ID', 'Bill Receiveable Person', 'PAN No', 'Item', 'Item_in', 'Item_out', 'Total', 'Taxable', 'VAT']
        df = pd.DataFrame(transaction.records , columns=headers)
        df.drop(columns=['Date AD', transaction.remove_col], axis=1, inplace=True)
        df.insert(6, 'unit', 'L')
        df.insert(8, 'blank', '')
        df['PAN No'].mask(df['PAN No'] == '', 000, inplace=True)
        df['PAN No'] = df['PAN No'].astype(int)
        df['PAN No'].mask(df['PAN No'] == 000, '', inplace=True)
        df[transaction.item_col] = df[transaction.item_col].astype(float)
        df['Total'] = df['Total'].astype(float)
        df['Taxable'] = df['Taxable'].astype(float)
        df['VAT'] = df['VAT'].astype(float)
        df.loc['Column_Total']= df.sum(numeric_only=True, axis=0)
        print(f"\n#### {transaction.transaction_name.capitalize()} transactions ####\n")
        print(df)

        # Totals
        total_taxable = round(df['Taxable'].iloc[-1], 2)
        print(f'\n[+] {transaction.transaction_name.capitalize()} total Taxable: {total_taxable}\n')

        PAN_customers_df = df[df['PAN No'].astype(bool)].copy()
        PAN_customers_df = PAN_customers_df.drop('Column_Total')
        PAN_customers_df = PAN_customers_df.groupby('PAN No').agg({'Bill Receiveable Person': 'first', 'Taxable': 'sum'}).reset_index()

        print(f'\n##### Transactions with PAN No. #####\n')
        print(PAN_customers_df)

        PAN_customers_df_filter = PAN_customers_df[PAN_customers_df['Taxable'].gt(1_00_000)].reset_index()

        for index, row in PAN_customers_df_filter.iterrows():
            append_transactions_above_1L(transactions_above_1L, row['PAN No'], row['Bill Receiveable Person'], transaction.trans_char, round(row['Taxable']))

        sheet = files[transaction.transaction_name]

        reader = pd.read_excel(sheet)
        with pd.ExcelWriter(
                sheet,
                mode="a",
                engine="openpyxl",
                if_sheet_exists="overlay",
                # engine_kwargs={'options': {'strings_to_numbers': True}},
            ) as writer:
                df.to_excel(writer, index=False, header=False,
                            sheet_name=transaction.sheet_name, startrow=len(reader) + 1)
    save_transactions_above_1L(files, transactions_above_1L)
            

if __name__ == '__main__':

    previous_month_np = get_previous_month_name_np()

    START_DATE_AD, END_DATE_AD = get_start_to_end_date_object_in_ad()

    START_DATE_BS, END_DATE_BS = get_start_to_end_date_object_in_bs()

    logger.info(f"#### Fetching transactions for {START_DATE_BS} to {END_DATE_BS} ({previous_month_np}) ####")

    trans_file = TransactionFileHandler(previous_month_np)
    files = trans_file.files

    purchase = Transaction(1, 'purchase', 'Nepali PB', 'Item_out', 'Item_in', 'P', None)
    sales = Transaction(2, 'sales', 'Nepali SB', 'Item_in', 'Item_out', 'S', None)

    transactions = Transactions(purchase, sales)

    main(transactions)