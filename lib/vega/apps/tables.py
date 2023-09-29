import os
import sys
sys.path.insert(0, "../lib")
import web3
import vega
import pandas as pd
from functools import lru_cache
from pathlib import Path
from typing import Callable
from . import log
from ..evm.web3 import ERC20TokenTracker
from ..db.sqlite import SQLiteDB


DATABASE_PATH = os.path.expandvars(f"$HOME/vega/data/dex.db") # not a good idea but ok for now

class Table:

    _table_name: str

    def reset(self):
        self._db.delete_table(self._table_name)


class TokenInfo(Table):

    _p: ERC20TokenTracker

    def __init__(self):

        self._db = SQLiteDB()
        self._db.connect(DATABASE_PATH)
        super(Table, self).__init__()
        self._p = ERC20TokenTracker()
        self._p.init()
        self._table_name = "token_info"
        self._index = ["addr"]
    
    def update_token_info(self, addr: str) -> None:
        addr = self._p.web3.to_checksum_address(addr)
        token_info = pd.DataFrame(self._p.gather_token_info(addr=addr), index=[0])
        self._db.write(token_info, index=["addr"], table_name=self._table_name)

    def touch(self, addr: str) -> None:
        """Touch it."""
        if not self.token_exists(addr):
            self.update_token_info(addr)
    
    def get_token_info(self, addr: str, touch=True) -> dict:
        if touch is True:
            self.touch(addr)
        addr = self.tocsaddr(addr)
        res = self._db.read(f"SELECT * from {self._table_name} WHERE addr = '{addr}'")
        return res.iloc[0].to_dict()
    
    def token_exists(self, addr) -> bool:
        addr = self.tocsaddr(addr)
        token_count = self.db.execute(f" SELECT COUNT(*) FROM {self.table_name} WHERE addr = '{addr}' ").fetchone()[0]
        assert token_count <= 1, f"found multiple tokens with addr {addr}"
        return token_count > 0
        
    @property
    def db(self) -> SQLiteDB:
        return self._db
    
    @property
    def table_name(self) -> str:
        return self._table_name

    @lru_cache(maxsize=None)
    def tocsaddr(self, addr) -> str:
        addr = self._p.web3.to_checksum_address(addr)
        return addr