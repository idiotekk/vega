import pandas as pd
from abc import ABC, abstractmethod
from typing import Union, List

from .. import log



class DataBase(ABC):

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def read_table(self, name):
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        pass

    @abstractmethod
    def write(self,
              df: pd.DataFrame,
              *,
              table_name: str):
        pass

    @abstractmethod
    def create_table(self, *,
                     table_name: str,
                     columns: List[str],
                     index=Union[str, List[str]]):
        pass