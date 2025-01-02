import asyncio
from logging import Logger
import sys

from constants.settings import STOCK_LOWER_PRICE, STOCK_UPPER_PRICE
from utils.load_financials import get_financial_df, load_urls
from constants.global_contexts import set_access_token
from utils.logger import get_logger
from utils.verify_symbols import get_correct_symbol

logger: Logger = get_logger(__name__)

# args = sys.argv[1].split(":")
# access_token = str(args[0])
access_token="prdRoEMC6KfEm5nmYP92aa1Xrv9qEn0d"

set_access_token(access_token=access_token)


async def load_financials():

    logger.info("TASK STARTED")

    await get_financial_df()

    logger.info("TASK ENDED")



if __name__ == "__main__":
    asyncio.run(load_financials())
    # asyncio.run(load_urls())
