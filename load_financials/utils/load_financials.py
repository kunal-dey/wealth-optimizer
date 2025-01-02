import asyncio
from os import getcwd

from time import sleep
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

from constants.settings import STOCK_NAME_PATH
from models.db_models.db_functions import find_by_name, retrieve_all_services
from models.financial import Financial
from utils.logger import get_logger
from logging import Logger


logger: Logger = get_logger(__name__)


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
        return None

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
        return None

    # Create the datetime object
    return datetime(year, month_num, datetime.now().day)


async def save_financials(stock: str, stored_financials_list: list, sec_code: str):
    logger.info(stock)
    sleep(2)
    financial = Financial(name=stock)

    response = requests.get(f"https://www.screener.in/company/{stock}/")

    if response.status_code == 404:
        response = requests.get(f"https://www.screener.in/company/{sec_code}/")
        if response.status_code == 404:
            logger.info(f"{stock} doesnt have url")

    if response and response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        quarter = soup.find(id='quarters')

        # to get dates
        dates_list = []
        for th in quarter.table.thead.tr:
            if th.text.replace('\n', '').replace(" ", ""):
                dates_list.append(th.text.replace('\n', '').replace(" ", ""))

        if len(dates_list) == 0:
            logger.info("dates invalid")
            logger.info(dates_list)
        else:
            financial.dates = dates_list

        # to get eps
        eps = quarter.find(string=re.compile("EPS in Rs")).parent.parent
        quarter_list = []
        for t in eps:
            el = t.text.replace('\n', '')
            if el:
                quarter_list.append(t.text)
        eps_list = [float(element.replace(",", "")) for element in quarter_list[1:]]

        if len(eps_list) == 0:
            logger.info("eps invalid")
            logger.info(eps_list)
        else:
            financial.eps = eps_list

        # to get sales
        sales = quarter.find(string=re.compile("Sales"))
        if sales:
            sales = sales.parent.parent.parent
        else:
            return None
        quarter_list = []
        for t in sales:
            el = t.text.replace('\n', '')
            if el:
                quarter_list.append(t.text)
        sales_list = [float(element.replace(",", "")) for element in quarter_list[1:]]

        if len(sales_list) == 0:
            logger.info("sales invalid")
            logger.info(sales_list)
        else:
            financial.sales = sales_list

        old_financial = list(filter(lambda x: x.name == stock, stored_financials_list))
        old_financial = old_financial[0] if len(old_financial) > 0 else None

        if financial.dates:
            financial.last_modified_date = parse_date_from_string(financial.dates[-1])

        if old_financial:
            financial.set_id(old_financial.get_id)
            if old_financial.dates == financial.dates:
                financial.last_modified_date = old_financial.last_modified_date
            else:
                financial.last_modified_date = datetime.now()
            if financial.eps and financial.sales:
                await financial.update_in_db()
                logger.info("Updated")
            else:
                logger.info(f"{stock} not Updated")
        else:
            if financial.eps and financial.sales:
                await financial.save_to_db()
                logger.info(f"{stock} Saved")
            else:
                logger.info(f"{stock} not Saved")


async def get_financial_df():
    routines = []
    stored_financials_list = await retrieve_all_services(Financial.COLLECTION, Financial)
    # for stock in stock_list:
    #     routines.append(save_financials(stock, stored_financials_list))

    market_cap_df = pd.read_csv(getcwd() + STOCK_NAME_PATH)
    logger.info(market_cap_df)
    for index in range(market_cap_df.shape[0]):

        symbol = market_cap_df["Symbol"].iloc[index]
        sec_code = market_cap_df["Security Code"].iloc[index]

        routines.append(save_financials(symbol, stored_financials_list, sec_code))

    await asyncio.gather(*routines)


async def load_urls():
    market_cap_df = pd.read_csv(getcwd() + STOCK_NAME_PATH)
    logger.info(market_cap_df)
    for index in range(market_cap_df.shape[0]):

        symbol = market_cap_df["Symbol"].iloc[index]
        sec_code = market_cap_df["Security Code"].iloc[index]

        response = requests.get(f"https://www.screener.in/company/{symbol}/")

        if response.status_code == 404:
            response = requests.get(f"https://www.screener.in/company/{sec_code}/")
            if response.status_code == 404:
                logger.info(f"{symbol} doesnt have url")
            else:
                logger.info(f"https://www.screener.in/company/{sec_code}/")
        else:
            logger.info(f"https://www.screener.in/company/{symbol}/")

        sleep(2)
