from models.stock_info import StockInfo
from dataclasses import dataclass, field

from logging import Logger

from utils.logger import get_logger

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from constants.settings import DEBUG

logger: Logger = get_logger(__name__)


@dataclass
class Stage:
    position_price: float
    quantity: int
    product_type: ProductType
    position_type: PositionType
    stock: None | StockInfo = None
    current_price: float = field(default=None, init=False)
    last_price: float = field(default=None, init=False)
    trigger: float | None = field(default=None)
    cost: float = field(default=None, init=False)

