import re
from datetime import datetime
from logging import Logger
from time import sleep
from dataclasses import dataclass, field
from typing import Callable

import requests
import pandas as pd
from bson import ObjectId
from dateutil.rrule import rrule, WEEKLY, MO, TU, WE, TH, FR

from constants.global_contexts import kite_context
from constants.settings import DEBUG, CURRENT_STOCK_EXCHANGE, TODAY
from models.db_models.object_models import get_save_to_db, get_delete_from_db, get_update_in_db

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def get_schema():
    return {
        "_id": "ObjectId",
        "stock_name": "str",
        "exchange": "str",
        "wallet": "float",
        "created_at": "datetime",
        "last_buy_price": "float",
        "last_quantity": "float",
        "first_load": "bool"
    }


@dataclass
class StockInfo:
    stock_name: str
    exchange: str = CURRENT_STOCK_EXCHANGE
    wallet: float = field(default=0.0)
    _id: ObjectId = field(default_factory=ObjectId)
    class_name: str = field(default="StockInfo", init=False)
    COLLECTION: str = field(default="index_stock_dbg" if DEBUG else "index_stock", init=False)
    latest_price: float = field(default=None, init=False)
    created_at: datetime = field(default=TODAY)
    __result_stock_df: pd.DataFrame | None = field(default=None, init=False)
    schema: dict = field(default_factory=get_schema, init=False)
    save_to_db: Callable = field(default=None, init=False)
    delete_from_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)
    quantity: int = field(default=0, init=False)
    first_load: bool = field(default=True)  # while loading from db make it true
    in_position: bool = field(default=False)
    last_buy_price: float = field(default=None)
    last_quantity: int = field(default=None)
    chosen_long_stocks: list = field(default=None)
    chosen_short_stocks: list = field(default=None)

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.delete_from_db = get_delete_from_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

