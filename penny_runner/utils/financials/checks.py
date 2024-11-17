import pandas as pd
from logging import Logger
from utils.logger import get_logger
from datetime import datetime, timedelta
import re

from models.financial import Financial
from models.db_models.db_functions import retrieve_all_services
from constants.settings import TODAY

logger: Logger = get_logger(__name__)

# Function to find month and year from the string and convert to datetime
def parse_date_from_string(date_str):

    # Month map with abbreviations for case-insensitive matching
    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
    }
    # Convert the input string to uppercase for case-insensitive matching
    date_str = date_str.upper()

    # Find the month abbreviation in the string
    month_num = None
    for month_abbr, month in month_map.items():
        if month_abbr in date_str:
            month_num = month
            break

    # If no month is found, raise an error
    if month_num is None:
        raise ValueError(f"Month not found in '{date_str}'")

    # Extract the year (4-digit or 2-digit year)
    year_match = re.search(r"(\d{4})|(\d{2})", date_str)
    if year_match:
        year_str = year_match.group()
        year = int(year_str)

        # Handle 2-digit year (e.g., '24' as 2024)
        if len(year_str) == 2:
            # Assume 2000s for simplicity (you can adjust this rule if needed)
            year += 2000 if year < 50 else 1900
    else:
        raise ValueError(f"Year not found in '{date_str}'")

    # Create the datetime object
    return datetime(year, month_num, datetime.now().day)


async def eps_and_sales_check():
    financial_list: list[Financial] = await retrieve_all_services("financial",Financial)
    filters = []
    for f in financial_list:
        if f.dates and f.eps:
            last_record_datetime = parse_date_from_string(f.dates[-1])
            if last_record_datetime + timedelta(days=35*3) > TODAY:
                if f.eps[-1] > f.eps[-2] and f.eps[-1] > 0:
                    if f.sales[-1] > 1.4*f.sales[-2] and f.eps[-2] > 0:
                        filters.append(f.name)
    logger.info(filters)
    return filters




def low_pe(stock_name: str, price_df: pd.DataFrame, eps_df: pd.DataFrame):
    if stock_name in price_df.columns and stock_name in eps_df.columns:
        stock_df = pd.merge(price_df[['Quarter', stock_name]], eps_df[['Quarter', stock_name]], on='Quarter', how='left')
        stock_df["pe"] = stock_df[f"{stock_name}_x"]/stock_df[f"{stock_name}_y"]
        if stock_df["pe"].iloc[-1] > 0:
            return stock_df["pe"].median() < 50
    return None


def increasing_eps(stock_name, eps):
    # return eps[stock_name].iloc[0] > eps[stock_name].iloc[1]
    return eps[stock_name].iloc[0] > 0


def increasing_sales(stock_name, sales):
    sales_list = list(sales[stock_name].values)
    return sales_list[0] > 1.04 * max(sales_list[1:])


def increasing_operating_profit(stock_name, operating_profit):
    return operating_profit[stock_name].iloc[0] > operating_profit[stock_name].iloc[1]


