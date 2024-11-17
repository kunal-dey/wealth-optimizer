from datetime import datetime
import os

DEBUG = os.environ.get("DEBUG", False)

YFINANCE_EXTENSION = "BO"
CURRENT_STOCK_EXCHANGE = "BSE"

# stocks name path
STOCK_NAME_PATH = "/temp/EQUITY_BSE.csv"
MARKET_CAP_HEADER_NAME = 'Market Capitalisation (Rs. Cr.)'

MIS_STOCK_LIST = "https://docs.google.com/spreadsheets/d/1fLTsNpFJPK349RTjs0GRSXJZD-5soCUkZt9eSMTJ2m4/export?format=csv"

TRAINING_DATE = datetime(2024, 9, 23) if DEBUG else datetime.now()

STOCK_LOWER_PRICE = os.environ.get("STOCK_LOWER_PRICE", 0)
STOCK_UPPER_PRICE = os.environ.get("STOCK_UPPER_PRICE", 30)
