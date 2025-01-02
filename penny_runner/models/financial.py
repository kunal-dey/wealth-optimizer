from dataclasses import dataclass, field
from logging import Logger
from typing import Callable

from bson import ObjectId
from datetime import datetime

from constants.settings import DEBUG
from models.db_models.object_models import get_save_to_db, get_update_in_db
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def get_schema():
    return {
        "_id": "ObjectId",
        "name": "str",
        "eps": "list",
        "dates": "list",
        "sales": "list",
        "last_modified_date": "datetime"
    }


@dataclass
class Financial:
    _id: ObjectId = field(default_factory=ObjectId)
    class_name: str = field(default="Financial", init=False)
    COLLECTION: str = field(default="financial_dbg" if DEBUG else "financial", init=False)
    schema: dict = field(default_factory=get_schema, init=False)
    save_to_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)
    name: str = field(default=None)
    eps: list = field(default=None)
    dates: list = field(default=None)
    sales: list = field(default=None)
    last_modified_date: datetime = field(default=None)

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

    def metrics(self):
        return {
            "name": self.name,
            "eps": self.eps,
            "sales": self.sales,
            "dates": self.dates,
            "last_modified_date": self.last_modified_date
        }
