import os
import sys
sys.path.insert(0, "../lib")
import web3
import vega
import pandas as pd
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional, List
from . import log
from ..evm.web3 import ERC20TokenTracker, Web3Portal, ContractEvent
from ..db.sqlite import SQLiteDB
from ..utils import apply_range


DATABASE_PATH = os.path.expandvars(f"$HOME/vega/data/dex.db") # not a good idea but ok for now

class Table:

    _table_name: str
    _p: Web3Portal
    _db: SQLiteDB

    def reset(self):
        self._db.delete_table(self._table_name)

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


class TokenInfo(Table):

    _p: ERC20TokenTracker

    def __init__(self):

        self._db = SQLiteDB()
        self._db.connect(DATABASE_PATH)
        self._p = ERC20TokenTracker()
        self._p.init()
        self._table_name = "token_info"
        self._index = ["addr"]
    
    def update_token(self, addr: str) -> None:
        addr = self.tocsaddr(addr)
        token_info = pd.DataFrame(self._p.gather_token_info(addr=addr), index=[0])
        self.db.write(token_info, index=["addr"], table_name=self.table_name)
        log.info(f"added {addr} to {self.table_name}")
    
    def delete_token(self, addr: str) -> None:
        self.db.execute(f"DELETE from {self.table_name} where addr = '{addr}'")
        self.db.con.commit()
        log.info(f"deleted {addr}")

    def touch(self, addr: str) -> None:
        """Touch it."""
        if not self.token_exists(addr):
            self.update_token(addr)
    
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


class EventArchive(Table):

    _p: ERC20TokenTracker

    def __init__(self, *,
                 table_name: str,
                 filter_params: Optional[dict]=None,
                 log_processor: Optional[Callable]=None,
                 event: Optional[ContractEvent]=None,
                 post_processor: Callable[pd.DataFrame, pd.DataFrame]=lambda x: x,
                 index: List[str]=["blockNumber", "logIndex"],
                 ):

        self._db = SQLiteDB()
        self._db.connect(DATABASE_PATH)
        self._p = ERC20TokenTracker()
        self._p.init()
        self._table_name = table_name
        self._index = index
        if filter_params is not None and log_processor is not None:
            self._filter_params = filter_params
            self._log_processor = log_processor
        elif event is not None:
            self._filter_params = event._get_event_filter_params(event.abi)
            self._log_processor = event.process_log
        else:
            raise ValueError(f"set (filter_params, log_processor) or event")
        self._post_process = post_processor
        self._index = index

    def fetch_range(self, *,
                    stime: pd.Timestamp,
                    etime: pd.Timestamp,
                    batch_freq: str,
                    write: bool=True,
                    ):

        def _fetch_range(stime: pd.Timestamp, etime: pd.Timestamp) -> pd.DataFrame:
            df = self._p.get_logs(
                stime=stime,
                etime=etime,
                filter_params=self._filter_params,
                log_processor=self._log_processor,
            )
            if len(df) == 0:
                return
            else:
                df = self._post_process(df)
                if write:
                    self.db.write(df, table_name=self._table_name, index=self._index)
                return df
        etime = min(pd.to_datetime(etime, utc=True), pd.Timestamp.utcnow())
        df = pd.concat(
            apply_range(
                func=_fetch_range,
                start=stime,
                end=etime,
                max_batch_size=pd.Timedelta(batch_freq)))
        return df
    
    def fetch_new(self,
                  *,
                  batch_freq: str,
                  time_col="timestamp",
                  ):

        stime = pd.to_datetime(token_info.db.read(f" SELECT MAX({time_col}) from {token_info.table_name}").iloc[0, 0])
        etime = pd.Timestamp.utcnow()
        self.fetch_range(stime=stime, etime=etime, batch_freq=batch_freq)


def event_archive_factory(name: str) -> EventArchive:

    weth = p.get_contract(addr=lookup("addr")["WETH"])
    post_processor = lambda df: df.drop(["blockHash", "address"], axis=1)
    if name == "weth_deposit":
        ea = EventArchive(
            table_name=name,
            event=weth.events["Deposit"](),
            post_processor=post_processor,
        )
    elif name == "weth_withdrawal":
        ea = EventArchive(
            table_name=name,
            event=weth.events["Withdrawal"](),
            post_processor=post_processor,
        )
    elif name == "weth_transfer":
        ea = EventArchive(
            table_name=name,
            event=weth.events["Transfer"](),
            post_processor=post_processor,
        )
    elif name == "token_transfer":
        erc20_transfer_topic = weth._get_event_filter_params(ea.abi)["topics"][0]
        e = weth.events["Transfer"]()
        def log_processor(raw_log):
            try:
                return e.process_log(raw_log)
            except:
                return {}
        ea = EventArchive(
            table_name=name,
            filter_params={"topics": [erc20_transfer_topic]},
            log_processor=log_processor,
            post_processor=post_processor,
        )
    else:
        raise ValueError(name)
    
    return ea