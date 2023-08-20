import datetime
from pyBSDate import bsdate, addate
import nepali_datetime


def get_end_date_of_previous_month(year: int = nepali_datetime.date.today().year, month: int = nepali_datetime.date.today().month):
    """ 
    Retruns the previous month's last date in B.S. in the format of 2079-12-30 
    Day is hardcoded to first day of passed current month.
    """
    previous_month_date = nepali_datetime.date(
        year, month, 1) - datetime.timedelta(days=1)
    return previous_month_date


def get_start_to_end_date_object_in_ad(end_date_of_a_month_np=get_end_date_of_previous_month()):
    """
    Returns tuple of (start_date: datetime.date, end_date: datetime.date) in AD format (conversion)
    """
    ne_date_start = bsdate(year=end_date_of_a_month_np.year, month=end_date_of_a_month_np.month, day=1)
    ne_date_end = bsdate(year=end_date_of_a_month_np.year,
                         month=end_date_of_a_month_np.month, day=end_date_of_a_month_np.day)
    en_date_start = ne_date_start.addate
    en_date_end = ne_date_end.addate
    return datetime.date(en_date_start.year, en_date_start.month, en_date_start.day), datetime.date(en_date_end.year, en_date_end.month, en_date_end.day)


def get_start_to_end_date_object_in_bs(end_date_of_a_month_np=get_end_date_of_previous_month()):
    """
    Returns tuple of (start_date: datetime.date, end_date: datetime.date) in BS format
    """
    ne_date_start = bsdate(year=end_date_of_a_month_np.year, month=end_date_of_a_month_np.month, day=1)
    ne_date_end = bsdate(year=end_date_of_a_month_np.year,
                         month=end_date_of_a_month_np.month, day=end_date_of_a_month_np.day)
    return ne_date_start, ne_date_end


def get_previous_month_name_np() -> str:
    """Returns the previous month name as per nepali calander"""
    return nepali_datetime._FULLMONTHNAMES[get_end_date_of_previous_month().month]