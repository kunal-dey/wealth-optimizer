import asyncio

from time import sleep
import requests
from bs4 import BeautifulSoup
import re

from models.financial import Financial
from utils.logger import get_logger
from logging import Logger


logger: Logger = get_logger(__name__)


async def save_financials(stock):
    logger.info(stock)
    sleep(2)
    retries = 0
    response = None
    financial = Financial(name=stock)
    while retries < 3:
        try:
            response = requests.get(f"https://www.screener.in/company/{stock}/")
            break
        except:
            sleep(2)
            retries += 1

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

    if financial.eps:
        await financial.save_to_db()


async def get_financial_df(stock_list, n):
    routines = []
    for stock in stock_list:
        routines.append(save_financials(stock))

    await asyncio.gather(*routines)
