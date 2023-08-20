from pathlib import Path
from os import scandir
import shutil
from openpyxl import load_workbook
from bs_ad_date_helpers import get_end_date_of_previous_month, get_fiscal_year_acc_prev_month_np

from my_logging import log_setup
import logging

log_setup()  # Initializing logging configurations
logger = logging.getLogger(__name__)

def delete_rows_in_excel(filePath: Path, index: list):
    import pandas as pd
    df = pd.read_excel(filePath)
    df = df.drop(df.index[index])
    df.to_excel(filePath, index=False)


class TransactionFileHandler():
    
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(TransactionFileHandler)
        return cls._instance

    def __init__(self, folder_name) -> None:
        self.cwd = Path.cwd()
        # Different directories
        self.sheetsD = self.cwd / 'sheets'
        self.formatD = self.sheetsD / 'format'
        self.create_dir_if_not_exists(self.formatD)
        self.transactionAbove1LD = self.formatD / 'transactionAbove1L'

        self.saveD = self.sheetsD / get_fiscal_year_acc_prev_month_np().replace('/', '-') /folder_name
        self.create_dir_if_not_exists(self.saveD)

        # Different files
        self.trans_above_1L_file = self.transactionAbove1LD / 'template.xls'
        self.transaction_above_1L_file_check(self.trans_above_1L_file)

        self.files = self.initialize_sheets(folder_name)

    def create_dir_if_not_exists(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)

    def copy(self, src, dest):
        """Copy the file from src to dest."""
        return shutil.copyfile(src, dest)
    
    def check_file_exists(self, path: Path):
        return path.exists()
    
    def add_report_details(self, src: Path, dest: Path, fiscal_year, month):
        desired = u'करदाता दर्ता नं (PAN) : 301003001        करदाताको नाम: SHANKER PARBATI OIL STORES         साल: {}    कर अवधि: {}'.format(fiscal_year, month)
        print(desired)
        workbook = load_workbook(filename=src)
        sheet = workbook.active
        sheet["A4"] = desired
        workbook.save(filename=dest)


    def transaction_above_1L_file_check(self, file_path: Path):
        if not self.check_file_exists(file_path):
            self.create_dir_if_not_exists(file_path.parent)
            import requests
            url = 'https://taxpayerportal.ird.gov.np/Sample%20Files/Transaction%20Above%20One%20Lakh%20Sample%20Document.xls'
            response = requests.get(url)
            if response.ok:
                file_path.write_bytes(response.content)
                logger.info('Successfully downloaded template file %s' % file_path)
                delete_rows_in_excel(file_path, [0, 1])
                logger.info('Cleaned up pre data')
            else:
                logger.error('Could not download template file for transaction above one lakhs')
        else:
            logger.info('Template file found for transaction above one lakh')
    

    def initialize_sheets(self, folder_name):
        """Copies the sheets to saveD to work with the sheets"""
        
        files = {}
        # Scanning the format directory to copy the initial sales-purchase sheets to saveD
        for entry in scandir(self.formatD):
            if entry.is_file():
                original_name = entry.name
                sheets_name = original_name.split(".")[0].split("-")[0]
                dest = self.saveD.joinpath(sheets_name + " - " + folder_name + ".xlsx")
                files[sheets_name] = dest
                month = get_end_date_of_previous_month().strftime('%m')
                fiscal_year = get_fiscal_year_acc_prev_month_np()
                # add report header details and also copy to the destination folder
                self.add_report_details(self.formatD.joinpath(original_name), dest, fiscal_year, month)
                # self.copy(self.formatD.joinpath(original_name), dest)
        
        trans_above_1L_file_dest = self.saveD / 'transactions_above_1L.xls'
        files['1L'] = trans_above_1L_file_dest
        self.copy(self.trans_above_1L_file, trans_above_1L_file_dest)
        return files
    