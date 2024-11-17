from logging import Logger

import yfinance as yf
import pandas as pd

from constants.enums.shift import Shift
from constants.settings import TRAINING_DATE
import numpy as np
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def generate_data(stock_df):
    """
        stock_df should contain price as one column
    """

    def get_slope(col):
        index = list(col.index)
        coefficient = np.polyfit(index, col.values, 1)
        ini = coefficient[0]*index[0]+coefficient[1]
        return coefficient[0]/ini

    def position(x):
        """
        given a series it finds whether there was increase of given value eg 1.05
        :param x:
        :return:
        """
        returns = (x.pct_change()+1).cumprod()
        return 0 if returns[returns > 1.05].shape[0] == 0 else 1

    col_with_period = {
        '6mo': 132,
        '3mo': 66,
        '1mo': 22,
        '5d': 5,
        '2d': 2
    }

    shifts = [sh for sh in range(3)]
    gen_cols = []

    for shift in shifts:
        for key, val in col_with_period.items():
            gen_cols.append(f"{key}_{shift}")
            stock_df.insert(
                len(stock_df.columns),
                f"{key}_{shift}",
                stock_df.reset_index(drop=True).shift(shift).price.rolling(val).apply(get_slope).values
            )

    stock_df.insert(
                len(stock_df.columns),
                'dir',
                stock_df.reset_index(drop=True).price.shift(-15).rolling(15).apply(lambda x: position(x)).values
            )
    gen_cols.append("dir")

    return stock_df[gen_cols].dropna()


def training_data(non_be_tickers: list, shift: Shift):
    """
    non_be_tickers: this should contain the list of all non -BE stocks to start with
    :return:
    """

    stocks_df = yf.download(tickers=non_be_tickers, interval='1d', period='1y')
    stocks_df.index = pd.to_datetime(stocks_df.index)
    stocks_df = stocks_df.loc[:str(TRAINING_DATE)]
    if shift == Shift.MORNING:
        stocks_df = stocks_df['Open'].bfill().ffill().dropna(axis=1)
    elif shift == Shift.EVENING:
        stocks_df = stocks_df['Close'].bfill().ffill().dropna(axis=1)

    stocks_list = list(stocks_df.columns)

    # generating the dataframe having both the input and output

    data_df = None
    for st in stocks_list:
        stock_df = stocks_df[[st]]
        stock_df.columns = ['price']
        if data_df is not None:
            data_df = pd.concat([data_df, generate_data(stock_df)]).reset_index(drop=True)
        else:
            data_df = generate_data(stock_df)
    return data_df

