import asyncio
from logging import Logger
import sys

from constants.settings import STOCK_LOWER_PRICE, STOCK_UPPER_PRICE
from utils.load_financials import get_financial_df
from utils.load_prices import get_price_df
from constants.global_contexts import set_access_token
from utils.logger import get_logger
from utils.verify_symbols import get_correct_symbol

logger: Logger = get_logger(__name__)

args = sys.argv[1].split(":")
access_token = str(args[0])

set_access_token(access_token=access_token)


async def load_financials():
    obtained_stock_list = await get_correct_symbol(lower_price=STOCK_LOWER_PRICE, higher_price=STOCK_UPPER_PRICE)
    obtained_stock_list = [st for st in obtained_stock_list if '-BE' not in st]
    logger.info(obtained_stock_list)
    price_df = get_price_df(obtained_stock_list)
    logger.info(price_df)

    await get_financial_df(obtained_stock_list, 7)


if __name__ == "__main__":
    asyncio.run(load_financials())
