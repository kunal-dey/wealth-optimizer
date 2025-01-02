import pandas as pd
from logging import Logger
from utils.logger import get_logger
from datetime import datetime, timedelta
import yfinance as yf
import re
import numpy as np

from models.financial import Financial
from models.db_models.db_functions import retrieve_all_services
from constants.settings import TODAY, TRAINING_DATE
from utils.indicators.kaufman_indicator import kaufman_indicator
from constants.settings import YFINANCE_EXTENSION

logger: Logger = get_logger(__name__)


def regression_line(data):
    x = np.array(np.arange(len(data)))
    y = np.array(data)

    x_mean = np.mean(x)
    y_mean = np.mean(y)

    m = np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean) ** 2)
    b = y_mean - m * x_mean

    return m / y_mean


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


async def eps_and_sales_check():
    financial_list: list[Financial] = await retrieve_all_services("financial", Financial)
    filters = []
    for f in financial_list:
        try:
            if f.last_modified_date and f.eps:
                last_record_datetime = f.last_modified_date
                if last_record_datetime is None:
                    continue
                if last_record_datetime + timedelta(days=35 * 3) > TODAY:
                    eps_ttm = [sum(f.eps[i - 3:i + 1]) if i + 1 > 3 else 0 for i in range(len(f.eps))]
                    if len(eps_ttm) > 6:
                        # logger.info(f.name)
                        # logger.info(eps_ttm[-6:])
                        # logger.info(regression_line(eps_ttm[-6:]))
                        # logger.info(regression_line(f.sales[-6:]))
                        if eps_ttm[-1] > eps_ttm[-2] > 0 and f.sales[-1] > 1.3 * f.sales[-2]:
                            if regression_line(eps_ttm[-6:]) > 0.05 and regression_line(f.sales[-6:]) > 0.05:
                                filters.append(f.name)

                        # if eps_ttm[-1] > 1.3 * eps_ttm[-2]:
                        #     if f.sales[-1] > 1.3 * f.sales[-2] and eps_ttm[-2] > 0:
                        #         filters.append(f.name)
        except:
            logger.exception(f"issue in eps for {f.name}")
    logger.info(filters)
    return filters


def get_quarters():
    current_date = TRAINING_DATE
    quarters = []

    for i in range(10):
        quarter_start = pd.Timestamp(current_date) - pd.offsets.QuarterEnd(startingMonth=3) * i
        if len(quarters) < 5:
            quarters.append(quarter_start)
    return quarters


async def low_pe_check(stock_list: list, price_df: pd.DataFrame):
    financial_list: list[Financial] = await retrieve_all_services("financial", Financial)
    filters = []
    for f in financial_list:
        if f.name in stock_list:
            eps_ttm = [sum(f.eps[i - 3:i + 1]) if i + 1 > 3 else 0 for i in range(len(f.eps))]
            eps_result = {}
            if len(list(reversed(eps_ttm[-5:]))) == 5:
                eps_result[f.name] = list(reversed(eps_ttm[-5:]))
            if len(eps_result) > 0:
                eps_df = pd.DataFrame(eps_result).transpose()
                eps_df.columns = get_quarters()
                eps_df = eps_df.transpose()
                eps_df.insert(len(eps_df.columns), 'Unnamed: 0', pd.to_datetime(eps_df.index))
                eps_df.insert(len(eps_df.columns), 'Quarter', eps_df['Unnamed: 0'].dt.to_period('Q'))
                eps_df.drop(['Unnamed: 0'], axis=1)

                stock_df = pd.merge(price_df[['Quarter', f.name]], eps_df[['Quarter', f.name]], on='Quarter',
                                    how='left')
                stock_df["pe"] = stock_df[f"{f.name}_x"] / stock_df[f"{f.name}_y"]

                stock_df.reset_index(inplace=True)
                pe_df = stock_df[["pe"]]
                pe_df.columns = ["price"]
                pe_df['line'] = pe_df.apply(kaufman_indicator)
                pe_df['ema'] = pe_df.line.ewm(span=5, adjust=False).mean()
                # logger.info(regression_line(pe_df['line'].dropna().values))
                if 0 > regression_line(pe_df['line'].dropna().values):

                    # if pe_df['ema'].iloc[-1] > pe_df['ema'].iloc[-3] < pe_df['ema'].iloc[-8]:
                    if 10 < stock_df["pe"].iloc[-1] < 25:
                        filters.append(f.name)
    logger.info(filters)
    return filters


def low_pe(stock_name: str, price_df: pd.DataFrame, eps_df: pd.DataFrame):
    if stock_name in price_df.columns and stock_name in eps_df.columns:
        stock_df = pd.merge(price_df[['Quarter', stock_name]], eps_df[['Quarter', stock_name]], on='Quarter',
                            how='left')
        stock_df["pe"] = stock_df[f"{stock_name}_x"] / stock_df[f"{stock_name}_y"]
        if stock_df["pe"].iloc[-1] > 0:
            return stock_df["pe"].median() < 50
    return None


async def decreasing_stocks_high_eps():
    def get_drawndown(raw_data, ending_date):
        min_drop, min_date, drop_days = None, None, None
        en_date = str(ending_date.date())
        test = raw_data.loc[:en_date].pct_change()
        test.columns = ["Close"]
        starting_index = raw_data.loc[:en_date].shape[0]
        if test[test["Close"] < -0.20].dropna().shape[0] > 0:
            fix_date = datetime.strptime(str(test[test["Close"] < -0.20].dropna().index[-1])[:-6], "%Y-%m-%d %H:%M:%S")
            starting_index = (ending_date - fix_date).days
        for days in range(0, starting_index):
            starting_date = ending_date - timedelta(days=days)
            st_date, en_date = str(starting_date.date()), str(ending_date.date())
            data = raw_data.loc[st_date:en_date]
            if min_drop is not None:

                if (data.pct_change(fill_method=None) + 1).cumprod().dropna().iloc[-1].values[0] < min_drop:
                    min_drop = (data.pct_change(fill_method=None) + 1).cumprod().dropna().iloc[-1].values[0]
                    min_date = starting_date
                    drop_days = days
            else:
                if (data.pct_change(fill_method=None) + 1).cumprod().dropna().shape[0] > 0:
                    min_drop = (data.pct_change(fill_method=None) + 1).cumprod().dropna().iloc[-1].values[0]
                    min_date = starting_date
                    drop_days = days

        return min_drop, min_date, drop_days

    financial_list: list[Financial] = await retrieve_all_services("financial", Financial)
    day_based_price_df = \
    yf.download(tickers=[f"{f.name}.{YFINANCE_EXTENSION}" for f in financial_list], period='6mo', interval='1d')[
        'Close']
    day_based_price_df = day_based_price_df.ffill().bfill()
    day_based_price_df.index = pd.to_datetime(day_based_price_df.index)
    price_df = day_based_price_df
    price_df = price_df.loc[:str(TRAINING_DATE.date())]
    price_df.columns = [st[:-3] for st in price_df.columns]
    price_df.insert(len(price_df.columns), 'Date', pd.to_datetime(price_df.index))
    price_df.insert(len(price_df.columns), 'Quarter', price_df['Date'].dt.to_period('Q'))
    price_df.ffill().bfill(inplace=True)

    def return_pe(stock_to_search):
        search_result = list(filter(lambda x: x.name == stock_to_search, financial_list))
        if len(search_result) == 1:
            f = search_result[0]
            eps_ttm = [sum(f.eps[i - 3:i + 1]) if i + 1 > 3 else 0 for i in range(len(f.eps))]
            eps_result = {}

            if len(list(reversed(eps_ttm[-5:]))) == 5:
                eps_result[f.name] = list(reversed(eps_ttm[-5:]))
            # logger.info(eps_result)
            if len(eps_result) > 0:
                eps_df = pd.DataFrame(eps_result).transpose()
                eps_df.columns = get_quarters()
                eps_df = eps_df.transpose()
                eps_df.insert(len(eps_df.columns), 'Unnamed: 0', pd.to_datetime(eps_df.index))
                eps_df.insert(len(eps_df.columns), 'Quarter', eps_df['Unnamed: 0'].dt.to_period('Q'))
                eps_df.drop(['Unnamed: 0'], axis=1)

                stock_df = pd.merge(price_df[['Quarter', f.name]], eps_df[['Quarter', f.name]], on='Quarter',
                                    how='left')
                stock_df["pe"] = stock_df[f"{f.name}_x"] / stock_df[f"{f.name}_y"]

                stock_df.reset_index(inplace=True)
                pe_df = stock_df[["pe"]]
                pe_df.columns = ["price"]

                return list(pe_df["price"])[-1]

    def increasing_eps(stock_to_search):
        search_result = list(filter(lambda x: x.name == stock_to_search, financial_list))
        if len(search_result) == 1:
            f = search_result[0]
            try:
                if f.last_modified_date and f.eps:
                    last_record_datetime = f.last_modified_date
                    if last_record_datetime is None:
                        return False
                    if last_record_datetime + timedelta(days=35 * 3) > TODAY:
                        eps_ttm = [sum(f.eps[i - 3:i + 1]) if i + 1 > 3 else 0 for i in range(len(f.eps))]
                        sales_ttm = [sum(f.sales[i - 3:i + 1]) if i + 1 > 3 else 0 for i in range(len(f.sales))]
                        if len(eps_ttm) > 6:
                            if eps_ttm[-1] > eps_ttm[-2] > 0 and sales_ttm[-1] > sales_ttm[-2]:
                                if regression_line(eps_ttm[-6:]) > 0.05 and regression_line(f.sales[-6:]) > 0.05:
                                    return True
                return False
            except:
                logger.exception(f"issue in eps for {f.name}")
                return False
    starting_date = datetime.now() - timedelta(days=1)

    for i in range(1):
        start = starting_date + timedelta(days=i)
        logger.info(start)

        drop_data = {"name": [], "drop": [], "drop_rate": [], "drop_date": [], "drop_days": []}
        for st in day_based_price_df.columns:
            drawn_data = get_drawndown(day_based_price_df[[st]], start)
            if drawn_data[0] is not None and drawn_data[2] is not None:
                drop_data["name"].append(st[:-3])

                drop_data["drop"].append(drawn_data[0])
                drop_data["drop_date"].append(drawn_data[1])
                drop_data["drop_days"].append(drawn_data[2])
                drop_data["drop_rate"].append((drawn_data[0]-1) / drawn_data[2])
        df = pd.DataFrame(drop_data)

        df["pe"] = df["name"].apply(lambda x: return_pe(x))
        df["eps"] = df["name"].apply(lambda x: increasing_eps(x))

        filter_df = df[(df["eps"] == True) & (df["drop"] < 0.65)].sort_values(by="drop", ascending=False)
        return list(filter_df["name"].values)
