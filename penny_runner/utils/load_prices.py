from logging import Logger

import pandas as pd
import yfinance as yf

from constants.settings import YFINANCE_EXTENSION, TRAINING_DATE

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def get_price_df(stock_list):

    try:
        yfinance_tickers = [f"{stock}.{YFINANCE_EXTENSION}" for stock in stock_list]
        price_df = yf.download(tickers=yfinance_tickers, period='1y', interval='1d')["Close"]
        price_df = price_df.ffill().bfill()
        price_df.index = pd.to_datetime(price_df.index)
        price_df = price_df.loc[:str(TRAINING_DATE.date())]
        logger.info(price_df)
        price_df.columns = [st[:-3] for st in price_df.columns]
        logger.info(price_df)
        price_df.insert(len(price_df.columns), 'Date', pd.to_datetime(price_df.index))
        price_df.insert(len(price_df.columns), 'Quarter', price_df['Date'].dt.to_period('Q'))
        price_df.ffill().bfill(inplace=True)
        price_df.to_csv(f"temp/financials/price_df.csv")
    except:
        try:
            price_df = pd.read_csv(f"temp/financials/price_df.csv", index_col=0)
        except:
            price_df = None
    return price_df
