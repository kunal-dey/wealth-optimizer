from datetime import datetime
import os

DEBUG = False

# time settings
__current_time = datetime.now()

TODAY = datetime(2024, 9, 23) if DEBUG else datetime.now()
TRAINING_DATE = datetime(2024, 9, 23) if DEBUG else datetime.now()

# stock limit
STOCK_LOWER_PRICE = 0
STOCK_UPPER_PRICE = 30000
CURRENT_STOCK_EXCHANGE = "NSE"
YFINANCE_EXTENSION = "NS"

# stocks name path
STOCK_NAME_PATH = "/temp/INDEX_NSE.csv"
MARKET_CAP_HEADER_NAME = 'Market Capitalisation'

def end_process():
    """
        function is used to end the process if price generator sends END PROCESS
    :return:
    """
    global END_PROCESS
    return END_PROCESS

def set_end_process(value):
    """
        function is used to end the process if price generator sends END PROCESS
    :param value:
    :return:
    """
    global END_PROCESS
    END_PROCESS = value
