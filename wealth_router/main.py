from datetime import datetime
from logging import Logger
import subprocess

from kiteconnect.exceptions import InputException
from quart import Quart, request, Blueprint
from quart_cors import cors
from time import sleep

from constants.global_contexts import set_access_token, kite_context

from models.db_models.db_functions import retrieve_all_services

from models.wallet import Wallet
from models.holding import Holding
from routes.wallet_input import wallet_input
from utils.logger import get_logger

app = Quart(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True
app = cors(app, allow_origin="*")

logger: Logger = get_logger(__name__)

token_list = []


async def get_allocated_funds():

    def get_available_cash():
        payload = kite_context.margins()
        return payload['equity']['available']['live_balance']

    wp, wg, wi = 0.2, 0.4, 0.4
    available_fund = get_available_cash()

    holding_list: list[Holding] = await retrieve_all_services("holding", Holding)
    penny_holding_list: list[Holding] = await retrieve_all_services("penny_holding", Holding)
    index_holding_list: list[Holding] = await retrieve_all_services("index_holding", Holding)

    holding_value = sum([h.quantity*h.position_price for h in holding_list])
    penny_holding_value = sum([h.quantity*h.position_price for h in penny_holding_list])
    index_holding_value = sum([h.quantity*h.position_price for h in index_holding_list])
    total_funds = holding_value+penny_holding_value+index_holding_value+available_fund

    available_fund_generic = max(total_funds*wg - holding_value, 0)
    available_fund_penny = max(total_funds*wp - penny_holding_value, 0)
    available_fund_index = max(total_funds*wi - index_holding_value, 0)

    proposed_available_fund = sum([available_fund_penny,available_fund_generic, available_fund_index])

    allocated_penny = (available_fund_penny/proposed_available_fund)*available_fund
    allocated_index = (available_fund_index/proposed_available_fund)*available_fund
    allocated_generic = (available_fund_generic/proposed_available_fund)*available_fund

    return {
        "generic": allocated_generic,
        "index": allocated_index,
        "penny": allocated_penny
    }


@app.get("/")
async def home():
    """
    home route
    :return:
    """
    return {"message": "Welcome to the Zerodha trading system"}


@app.route("/time")
def get_time():
    """
        route for checking the date and time of the server. Required for scheduling trading time
    :return:
    """
    return {"current_time": datetime.now()}


@app.get("/set")
async def set_token_request():
    """
    route to set the access token which is received after login zerodha using starter app
    :return:
    """
    global logger
    try:
        set_access_token(request.args["token"])
        logger.info("TOKEN HAS BEEN SET")
        return {"message": "Token set"}
    except:
        return {"message": "there is an error"}


@app.get("/start")
async def start_process():
    """
    route checks whether login has been done and then starts docker containers for different processes
    :return:
    """
    global logger

    def single_process(service_name, arg_list: list):
        try:
            args = ":".join(arg_list)
            subprocess.Popen(["sudo", "systemctl", "enable", f"{service_name}@{args}"])
            sleep(5)
            subprocess.Popen(["sudo", "systemctl", "start", f"{service_name}@{args}"])
            message = f"{service_name} started successfully."
        except subprocess.CalledProcessError as e:
            message = f"Failed to restart {service_name}: {e}"
        logger.info(message)
        return message

    try:
        # to test whether the access toke has been set after login
        _ = kite_context.ltp("NSE:INFY")

        # starting the background task which will run the entire process
        service_message = None
        allocated_funds = await get_allocated_funds()
        match request.args["task"]:
            case "training":
                # docker container which is running the training operation
                service_message = "To be added"
            case "load-financials":
                service_message = single_process(service_name="load_financials", arg_list=[kite_context.access_token])
            case "penny":
                arg_list = [kite_context.access_token, str(allocated_funds["penny"]), "runner"] # access_token, wallet, mode(training or runner)
                service_message = single_process(service_name="penny", arg_list=arg_list)
            case "index":
                arg_list = [kite_context.access_token, str(allocated_funds["index"]), "runner"]
                service_message = single_process(service_name="index", arg_list=arg_list)
            case "generic":
                # arg_list = [kite_context.access_token, allocated_funds["generic"], "runner"]
                # service_message = single_process(service_name="generic", arg_list=arg_list)
                service_message = "To be added"
            case "all":
                service_message = ""
                arg_list = [kite_context.access_token, str(allocated_funds["penny"]), "runner"]
                service_message += "\n" + single_process(service_name="penny", arg_list=arg_list)
                arg_list = [kite_context.access_token, str(allocated_funds["index"]), "runner"]
                service_message += "\n" + single_process(service_name="index", arg_list=arg_list)
                # arg_list = [kite_context.access_token, allocated_funds["generic"], "runner"]
                # service_message += "\n" + single_process(service_name="generic", arg_list=arg_list)
        token_list.append(kite_context.access_token)
        return {"message": service_message}
    except InputException:
        return {"message": "Kindly login first"}


# @app.route("/stop")
# async def stop_background_tasks():
#     """
#         On being deployed if we need to manually stop any specific container we can do using this route
#     """
#     global logger
#
#     def single_process(service_name):
#         try:
#             subprocess.Popen(["sudo", "rm", "-rf", "/etc/systemd/system/load_financials@.service"], check=True)
#             message = f"{service_name} stopped successfully."
#         except subprocess.CalledProcessError as e:
#             message = f"Failed to stop {service_name}: {e}"
#         logger.info(message)
#         return message
#
#     service_message = None
#     match request.args["task"]:
#         case "training":
#             # docker container which is running the training operation
#             service_message = "To be added"
#         case "load-financials":
#             service_message = single_process(service_name="financials")
#         case "penny":
#             service_message = single_process(service_name="penny")
#         case "index":
#             service_message = single_process(service_name="index")
#         case "generic":
#             # service_message = single_process(service_name="generic")
#             service_message = "To be added"
#         case "all":
#             service_message = ""
#             service_message += "\n" + single_process(service_name="penny")
#             service_message += "\n" + single_process(service_name="index")
#             # service_message += "\n" + single_process(service_name="generic")
#     return {"message": service_message}


@app.get("/create-wallet")
async def create_wallet():
    wallet: Wallet = Wallet()
    await wallet.create_wallet()
    return {"msg": "wallet"}

resource_list: list[Blueprint] = [wallet_input]

for resource in resource_list:
    app.register_blueprint(blueprint=resource)

if __name__ == "__main__":
    app.run(port=8081)

