import asyncio
import os
from asyncio import sleep
from datetime import datetime
from logging import Logger
import sys
import yfinance as yf
import pandas as pd

from constants.enums.shift import Shift
from constants.settings import STOCK_LOWER_PRICE, STOCK_UPPER_PRICE, set_wallet_value, get_allocation, \
    YFINANCE_EXTENSION, TRAINING_DATE, DEBUG, get_wallet_value, START_TIME, SLEEP_INTERVAL, END_TIME, end_process, \
    set_end_process, STOP_BUYING_TIME_MORNING, START_BUYING_TIME_MORNING, STOP_BUYING_TIME_EVENING, \
    START_BUYING_TIME_EVENING, set_max_stocks, get_max_stocks, CURRENT_STOCK_EXCHANGE, EXPECTED_MINIMUM_MONTHLY_RETURN
from constants.global_contexts import set_access_token
from models.account import Account
from models.db_models.db_functions import retrieve_all_services, find_by_name
from models.stages.position import Position
from models.stock_info import StockInfo
from models.wallet import Wallet
from utils.logger import get_logger
from utils.tracking_components.fetch_prices import fetch_current_prices
from utils.tracking_components.verify_symbols import get_correct_symbol
from utils.financials.checks import eps_and_sales_check, low_pe_check, decreasing_stocks_high_eps

logger: Logger = get_logger(__name__)

args = sys.argv[1].split(":")
access_token = str(args[0])
wallet_value_params = float(args[1])
mode = str(args[2])

set_access_token(access_token=access_token)
set_wallet_value(amount=wallet_value_params)


async def background_task():
    """
        all the tasks mentioned here will be running in the background
    """
    global logger

    logger.info("BACKGROUND TASK STARTED")

    account: Account = Account()

    account.available_cash = get_wallet_value()
    account.starting_cash = get_wallet_value()

    # prediction_df columns contains .NS whereas obtained_stock_list has all stocks which can be traded and are in mis
    prediction_df, obtained_stock_list = None, await get_correct_symbol(lower_price=STOCK_LOWER_PRICE, higher_price=STOCK_UPPER_PRICE)
    logger.info(f"listed of stocks : {obtained_stock_list}")

    not_loaded = True
    # filtered stocks contains all stocks in prediction_df with .NS removed
    filtered_stocks, selected_long_stocks, selected_short_stocks = [], [], []

    # variable to check if minimum return of 0.005 is obtained in a day then free 2/3 of the portfolio for next day
    today_profit = 0

    """
    START OF DAY ACTIVITIES
    """

    # fetch all the stocks already added in stock list
    stock_list: list[StockInfo] = await retrieve_all_services(StockInfo.COLLECTION, StockInfo)
    logger.info(f" list of all stocks to add: {stock_list}")
    logger.info(f" per stock allocation: {get_allocation()}")

    # load all the stock objects to stock to track
    for stock_obj in stock_list:
        account.stocks_to_track[stock_obj.stock_name] = stock_obj

    # load all holdings from the database
    await account.load_holdings()

    # loading day based price df from yahoo finance
    day_based_price_df = None

    try:
        day_based_price_df = yf.download(tickers=[f"{st}.{YFINANCE_EXTENSION}"for st in obtained_stock_list], period='1y', interval='1d')[['Close', 'High', 'Low', 'Open']]
        day_based_price_df = day_based_price_df.ffill().bfill()
        day_based_price_df.index = pd.to_datetime(day_based_price_df.index)
        day_based_price_df = day_based_price_df.loc[:str(TRAINING_DATE.date())]
        logger.info(day_based_price_df)
        day_based_price_df.reset_index(drop=True, inplace=True)
        day_based_price_df.to_csv(f"temp/day_based_price_df.csv")
    except:
        try:
            day_based_price_df = pd.read_csv(f"temp/day_based_price_df.csv", index_col=0)
        except:
            day_based_price_df = None

    # loading day based price df from yahoo finance
    try:
        prediction_df = yf.download(tickers=[f"{st}.{YFINANCE_EXTENSION}"for st in obtained_stock_list], period='5d', interval='1m')['Close']
        prediction_df = prediction_df.ffill().bfill()
        prediction_df.index = pd.to_datetime(prediction_df.index)
        prediction_df = prediction_df.loc[:str(TRAINING_DATE.date())]
        prediction_df.reset_index(drop=True, inplace=True)
        prediction_df.to_csv(f"temp/prediction_df.csv")
    except:
        try:
            prediction_df = pd.read_csv(f"temp/prediction_df.csv", index_col=0)
        except:
            prediction_df = None

    # eliminating uncommon stocks
    stocks_present = []

    if prediction_df is not None and day_based_price_df is not None:
        day_based_col = {st for _, st in day_based_price_df.columns}
        for a in [i for i in list(prediction_df.columns)]:
            for b in list(day_based_col):
                if a == b:
                    stocks_present.append(a)

        # filtering the stock to contain only the available stocks
        prediction_df = prediction_df[stocks_present]
        day_col = [(price_seg, st) for price_seg in ['Close', 'High', 'Low', 'Open'] for st in stocks_present]
        day_based_price_df = day_based_price_df[day_col]

        prediction_df = prediction_df.iloc[-1:]
        prediction_df.reset_index(drop=True, inplace=True)

    logger.info(prediction_df)

    # loading all holdings and stocks into a list to compare what has been sold at the end
    # these are just used for verification at the end

    initial_list_of_holdings = list(account.holdings.keys())
    initial_list_of_stocks = list(account.stocks_to_track.keys())

    # setting the starting cash and available cash
    account.starting_cash = account.starting_cash + len(initial_list_of_holdings)*get_allocation()
    account.available_cash = account.available_cash - (len(initial_list_of_stocks)-len(initial_list_of_holdings))*get_allocation()

    # this is done as mostly we are storing holding and trade occurs as positions
    account.convert_holdings_to_positions()

    logger.info(f"starting cash: {account.available_cash}")

    blacklisted_stocks = []

    eps_check_result = await decreasing_stocks_high_eps()

    # this part will loop till the trading times end
    current_time = datetime.now()

    while current_time < END_TIME:
        current_time = datetime.now()

        # if the trading has not started then iterate every 1 sec else iterate every 30 sec
        if START_TIME < current_time:
            await sleep(SLEEP_INTERVAL)
        else:
            await sleep(1)

        # even if any error occurs it will not break it
        try:

            if not_loaded and current_time >= START_TIME:
                filtered_stocks = [i[:-3] for i in list(prediction_df.columns)]

                logger.info(f"list of filtered stocks: {filtered_stocks}")
                not_loaded = False

            if end_process():
                break

            """
                if the time is within trading interval or certain criteria is met then buy stocks
            """

            if START_TIME < current_time:
                # update the prediction_df after every interval
                new_cost_df = await fetch_current_prices(filtered_stocks)
                # the if condition is for the debug process
                if new_cost_df is None:
                    set_end_process(True)
                else:
                    prediction_df = pd.concat([prediction_df, new_cost_df], ignore_index=True)
                    prediction_df = prediction_df.bfill().ffill()
                    prediction_df.dropna(axis=1, inplace=True)
                    prediction_df = prediction_df[[col for col in list(prediction_df.columns) if '-BE' not in col]]
                    prediction_df = prediction_df.iloc[-2000:]
                    prediction_df = prediction_df.reset_index(drop=True)
                    price_filter = list(prediction_df.iloc[-1][prediction_df.iloc[-1] < 30].index)
                    prediction_df = prediction_df[price_filter]
                    prediction_df.to_csv(f"temp/prediction_df.csv")

                # # listing those stocks first with less VaR
                data_resampled = prediction_df.iloc[::60, :]
                log_returns = data_resampled.pct_change()
                VaR_95 = log_returns.quantile(0.005, interpolation='lower')

                filtered_stock_list = [f"{st}.NS" for st in eps_check_result]

                predicted_stocks = list(VaR_95[filtered_stock_list].sort_values(ascending=False).index)

                selected_long_stocks = [st[:-3] for st in predicted_stocks]

                logger.info(f"chosen long: {selected_long_stocks}")

                logger.info(f"available cash : {account.available_cash}")

                logger.info(f"list of the stocks to track: {account.stocks_to_track.keys()}")

                if (STOP_BUYING_TIME_MORNING > current_time > START_BUYING_TIME_MORNING) or (STOP_BUYING_TIME_EVENING > current_time > START_BUYING_TIME_EVENING):

                    # selecting stock which meets the criteria
                    for stock_col in selected_long_stocks:
                        if stock_col not in blacklisted_stocks:
                            # available cash keeps on changing so max_stocks keeps on changing
                            # the stock will be added if it is added for the first time
                            set_max_stocks(int(account.available_cash/get_allocation()))
                            if 0 < get_max_stocks() and stock_col not in account.stocks_to_track.keys():

                                raw_stock = StockInfo(stock_col, CURRENT_STOCK_EXCHANGE)
                                if not DEBUG:
                                    sell_orders: list = raw_stock.get_quote["sell"]
                                    zero_quantity = True
                                    for item in sell_orders:
                                        if item['quantity'] > 0:
                                            zero_quantity = False
                                        break
                                    if zero_quantity:
                                        continue
                                account.stocks_to_track[stock_col] = raw_stock
                                _, _2 = account.stocks_to_track[stock_col].buy_parameters()
                                # even if it may seem that allocation is reduced when bought, actual change is while adding the
                                # stock in stocks to track
                                account.available_cash -= get_allocation()
                                stock_df = prediction_df[[f"{stock_col}.{YFINANCE_EXTENSION}"]]
                                stock_df.reset_index(inplace=True, drop=True)
                                stock_df = stock_df[[f"{stock_col}.{YFINANCE_EXTENSION}"]].bfill().ffill()
                                stock_df.columns = ['price']
                                stock_df.to_csv(f"temp/{stock_col}.csv")

                                logger.info("whether actually the stock df has all the data or not")
                                logger.info(f"{stock_col}: {stock_df.shape}")

                """
                    update price for all the stocks which are being tracked
                """

                for stock in account.stocks_to_track.keys():
                    logger.info(f"stock to update {stock}")
                    account.stocks_to_track[stock].update_price()

                if STOP_BUYING_TIME_MORNING > current_time > START_BUYING_TIME_MORNING:
                    try:
                        account.buy_stocks(day_based_price_df, shift=Shift.MORNING)
                    except:
                        pass

                elif STOP_BUYING_TIME_EVENING > current_time > START_BUYING_TIME_EVENING:
                    try:
                        account.buy_stocks(day_based_price_df, shift=Shift.EVENING)
                    except:
                        pass

                """
                    if the trigger for selling is breached in position then sell
                """

                positions_to_delete = []  # this is needed or else it will alter the length during loop

                for position_name in account.positions.keys():
                    position: Position = account.positions[position_name]
                    status = position.breached()
                    match status:
                        case "SELL_PROFIT":
                            logger.info(f" profit -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                            logger.info(f"{position.stock.wallet/get_allocation()}, {(1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(position.stock.number_of_days/20)}")
                            # if 1+(position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(position.stock.number_of_days/20):
                            logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                            # if its in holding then fund is added next day else for position its added same day
                            if position.stock.number_of_days == 1:
                                account.available_cash += get_allocation()
                            os.remove(os.getcwd() + f"/temp/{position_name}.csv")
                            today_profit += float(account.stocks_to_track[position_name].wallet)
                            positions_to_delete.append(position_name)

                        case "SELL_LOSS":
                            logger.info(f" loss -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                            positions_to_delete.append(position_name)
                            os.remove(os.getcwd() + f"/temp/{position_name}.csv")
                            if position.stock.number_of_days == 1:
                                account.available_cash += get_allocation()
                            today_profit += float(account.stocks_to_track[position_name].wallet)
                            logger.info(f"blacklisted stocks: {blacklisted_stocks}")

                        case "CONTINUE":
                            continue

                for position_name in positions_to_delete:
                    del account.positions[position_name]
                    del account.stocks_to_track[position_name]  # delete from stocks to track

        except:
            logger.exception("Kite error may have happened")

    # sell all the stocks which has trigger and is not None

    positions_to_delete = []  # this is needed or else it will alter the length during loop

    # to store all the stock with wallet value in ascending order
    wallet_order = {}

    for position_name in account.positions.keys():
        position: Position = account.positions[position_name]
        if position.stock.number_of_days == 1:
            if position.trigger is None:
                if position.stock.latest_price > position.cost:
                    if position.sell():
                        logger.info(f" crossed the cost -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                        positions_to_delete.append(position_name)
                        account.available_cash += get_allocation()
                        today_profit += float(account.stocks_to_track[position_name].wallet)
                        # del account.stocks_to_track[position_name]  # delete from stocks to track
                else:
                    tx_cost = position.stock.transaction_cost(buying_price=position.position_price, selling_price=position.current_price) / position.quantity
                    wallet_value = (position.current_price - (position.position_price + tx_cost)) * position.quantity
                    wallet_order[wallet_value] = position_name
            else:
                if position.sell():
                    logger.info(f" BREACHED at the end of day -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                    positions_to_delete.append(position_name)
                    logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                    account.available_cash += get_allocation()
                    today_profit += float(account.stocks_to_track[position_name].wallet)
        else:
            if 1+(position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(position.stock.number_of_days/20):
                if position.sell():
                    positions_to_delete.append(position_name)
                    logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                    account.available_cash += get_allocation()
                    today_profit += float(account.stocks_to_track[position_name].wallet)
                    # del account.stocks_to_track[position_name]  # delete from stocks to track

    for position_name in positions_to_delete:
        del account.positions[position_name]
        if account.stocks_to_track[position_name].wallet > 0:
            del account.stocks_to_track[position_name]  # delete from stocks to track
            os.remove(os.getcwd() + f"/temp/{position_name}.csv")

    sorted_wallet_list = list(wallet_order.keys())

    positions_to_delete = []

    wallets = await retrieve_all_services(Wallet.COLLECTION, Wallet)
    wallet_obj: Wallet = wallets[0]

    logger.info(sorted(sorted_wallet_list))

    for wallet_v in sorted(sorted_wallet_list):
        position_name = wallet_order[wallet_v]
        position: Position = account.positions[position_name]
        logger.info(wallet_obj.accumulated_amount - wallet_obj.expected_amount)
        logger.info(wallet_v)

        try:
            if abs(float(wallet_v)) < wallet_obj.accumulated_amount - wallet_obj.expected_amount and position.stock.number_of_days > 16:
                if position.sell(force=True):
                    logger.info(f" sold as the stock was there for 16 days and accumulated amount is more {position.stock.stock_name} at {position.stock.latest_price}")
                    positions_to_delete.append(position_name)
                    logger.info(f"stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                    wallet_obj.accumulated_amount -= account.stocks_to_track[position_name].wallet
                    logger.info(wallet_obj.accumulated_amount)
                    account.available_cash += get_allocation()
                    today_profit += float(account.stocks_to_track[position_name].wallet)
                    del account.stocks_to_track[position_name]  # delete from stocks to track
        except:
            logger.exception("failed in wallet check")

    for position_name in positions_to_delete:
        del account.positions[position_name]
        os.remove(os.getcwd() + f"/temp/{position_name}.csv")

    """
        END OF DAY ACTIVITIES
    """

    # if the stock is above trigger it should be sold or the cost will increase next day and the fund will also be trapped

    # stock information is stored in the db.
    # this is from an older code where the holdings weren't present, but the stock was tracked for more than 1 day.
    for stock_key in account.stocks_to_track.keys():
        """
            if the remaining stock to track is already available update it or else add a new record in db
        """
        stock_model = await find_by_name(StockInfo.COLLECTION, StockInfo, {"stock_name": f"{stock_key}"})
        if stock_model is None:
            await account.stocks_to_track[stock_key].save_to_db()
        else:
            await account.stocks_to_track[stock_key].update_in_db()

    await account.remove_all_sold_stocks(initial_list_of_stocks)

    # Since only positions are stored in the database, so first positions are converted to holdings.
    # After which new holdings are added and old holdings are updated.
    await account.store_all_holdings()

    # deleting all holding data from db which have been sold
    await account.remove_all_sold_holdings(initial_list_of_holdings)

    logger.info(f"Today's profit {today_profit}")

    logger.info("TASK ENDED")

if __name__ == "__main__":
    if mode == "runner":
        asyncio.run(background_task())
    else:
        logger.info("Wrong service to run")
