import os
import argparse
import pandas as pd
from functools import lru_cache
from typing import Callable, Optional, List
import pymongo

from . import log
from ..evm.web3 import ERC20TokenTracker, Web3Portal, ContractEvent
from ..evm.utils import lookup
from ..db import DataBase
from ..db.mongo import MongoDB
from ..utils import apply_range


DATABASE_PATH = os.path.expandvars(f"$HOME/vega/data/dex.db") # not a good idea but ok for now

class Table:

    _table_name: str
    _p: Web3Portal
    _d: DataBase

    def reset(self):
        self._d.delete_table(self._table_name)

    @property
    def d(self) -> DataBase:
        return self._d
    
    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def table():
        return self.con[self.table_name]

    @lru_cache(maxsize=None)
    def tocsaddr(self, addr) -> str:
        addr = self._p.web3.to_checksum_address(addr)
        return addr


class TokenInfo(Table):

    _d: DataBase
    _p: ERC20TokenTracker

    def __init__(self, *, d: DataBase):

        self._d = d
        self._p = ERC20TokenTracker()
        self._p.init()
        self._table_name = "token_info"
        self._index = ["addr"]
    
    def update_token(self, addr: str) -> None:
        addr = self.tocsaddr(addr)
        token_info = pd.DataFrame(self._p.gather_token_info(addr=addr), index=[0])
        token_info["totalSupply"] = token_info["totalSupply"].astype(str) # for mongo
        self.d.write(token_info, index=["addr"], table_name=self.table_name, update=True)
        log.info(f"added {addr} to {self.table_name}")
    
    def delete_token(self, addr: str) -> None:
        if isinstance(self.d, MongoDB):
            self.d.table(self.table_name).delete_one({"addr": addr})
        else:
            self.d.execute(f"DELETE from {self.table_name} where addr = '{addr}'")
            self.d.con.commit()
            log.info(f"deleted {addr}")

    def touch(self, addr: str) -> None:
        """Touch it."""
        if not self.d.table_exists(self.table_name) or not self.token_exists(addr):
            self.update_token(addr)
    
    def get_token_info(self, addr: str, touch=True) -> dict:
        if touch is True:
            self.touch(addr)
        addr = self.tocsaddr(addr)
        res = self._d.read_sql(f"SELECT * from {self._table_name} WHERE addr = '{addr}'")
        return res.iloc[0].to_dict()
    
    def token_exists(self, addr) -> bool:
        addr = self.tocsaddr(addr)
        if isinstance(self.d, MongoDB):
            return self.d.table(self.table_name).find_one({"addr": addr}) is not None
        else:
            token_count = self.d.execute(f" SELECT COUNT(*) FROM {self.table_name} WHERE addr = '{addr}' ").fetchone()[0]
            assert token_count <= 1, f"found multiple tokens with addr {addr}"
            return token_count > 0
        
    @property
    def d(self) -> DataBase:
        return self._d
    
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
                 d: DataBase,
                 table_name: str,
                 filter_params: Optional[dict]=None,
                 log_processor: Optional[Callable]=None,
                 event: Optional[ContractEvent]=None,
                 post_processor: Callable[pd.DataFrame, pd.DataFrame]=lambda x: x,
                 index: List[str]=["blockNumber", "logIndex"],
                 ):

        self._d = d
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
                    sblock: Optional[int]=None,
                    eblock: Optional[int]=None,
                    stime: Optional[pd.Timestamp]=None,
                    etime: Optional[pd.Timestamp]=None,
                    batch_freq: str,
                    write: bool=True,
                    return_df: bool=False,
                    ) -> Optional[pd.DataFrame]:
        if sblock:
            stime = self._p.get_timestamp_from_block_number(sblock)
        if eblock:
            etime = self._p.get_timestamp_from_block_number(eblock)

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
                    self.d.write(df, table_name=self._table_name, index=self._index)
                if return_df:
                    return df
        etime = min(pd.to_datetime(etime, utc=True), pd.Timestamp.utcnow())
        res = apply_range(
                func=_fetch_range,
                start=stime,
                end=etime,
                max_batch_size=pd.Timedelta(batch_freq))
        if return_df:
            df = pd.concat(res)
            return df
        else:
            import gc
            gc.collect()
    
    def fetch_new(self,
                  *,
                  batch_freq: str,
                  block_number_col: str="blockNumber",
                  ):
        """Bootstrap from max block in the table.
        """
        #stime = pd.to_datetime(self.d.read_sql(f" SELECT MAX({time_col}) from {token_info.table_name}").iloc[0, 0])
        sblock = int(self.d.con[self.table_name].find().sort(block_number_col, -1).limit(1)[0][block_number_col]) - 1
        stime = self._p.get_timestamp_from_block_number(sblock)
        log.info(f"bootstrapping from block {sblock} {stime}")
        etime = pd.Timestamp.utcnow()
        self.fetch_range(stime=stime, etime=etime, batch_freq=batch_freq)


def event_archive_factory(name: str) -> EventArchive:
    """Create commonly used archives.
    """

    p = ERC20TokenTracker()
    p.init()
    d = MongoDB()
    d.init()
    weth = p.get_contract(addr=lookup("addr")["WETH"])

    def try_process_log(e):
        def log_processor(raw_log: dict) -> dict:
            try:
                return e.process_log(raw_log)
            except:
                return {}
        return log_processor
    def post_process(
            df: pd.DataFrame,
            output_cols: List[str]=["src", "dst", "amount", "transactionHash", "blockNumber"],
            ) -> pd.DataFrame:
        df = df.rename(columns={"args_src": "src", "args_dst": "dst", "args_wad": "amount"})
        output_cols = [_ for _ in output_cols if _ in df.columns]
        return df[output_cols].copy()

    if name == "weth_deposit":
        d.con[name].create_index("dst")
        ea = EventArchive(
            d=d,
            table_name=name,
            event=weth.events["Deposit"](),
            post_processor=post_process,
        )
        ea.d.con[name].create_index("addr")
    elif name == "weth_withdrawal":
        d.con[name].create_index("src")
        ea = EventArchive(
            d=d,
            table_name=name,
            event=weth.events["Withdrawal"](),
            post_processor=post_process,
        )
    elif name == "weth_transfer":
        d.con[name].create_index("src")
        d.con[name].create_index("dst")
        ea = EventArchive(
            d=d,
            table_name=name,
            event=weth.events["Transfer"](),
            post_processor=post_process,
        )
    elif name == "token_transfer":
        e = weth.events["Transfer"]()
        d.con[name].create_index("src")
        d.con[name].create_index("dst")
        d.con[name].create_index("address")
        erc20_transfer_topic = e._get_event_filter_params(e.abi)["topics"][0]
        ea = EventArchive(
            d=d,
            table_name=name,
            filter_params={"topics": [erc20_transfer_topic]},
            log_processor=try_process_log(e),
            post_processor=lambda df:
                post_process(
                    df,
                    output_cols=["address", "src", "dst", "amount", "transactionHash", "blockNumber"]),
        )
    elif name == "uniswap_v2_swap":

        token_addr = lookup("addr")["BITCOIN"]
        pool = p.get_univswap_v2_pair(addr=token_addr)
        abi = p.get_abi(addr=pool)
        c = p.web3.eth.contract(address=pool, abi=abi)
        e = c.events["Swap"]()
        swap_topic = e._get_event_filter_params(e.abi)["topics"][:1]

        def post_processor(df: pd.DataFrame) -> pd.DataFrame:
            df["amount0"] = df["args_amount0In"] - df["args_amount0Out"]
            df["amount1"] = df["args_amount1In"] - df["args_amount1Out"]
            return df.drop(["args_amount0In", "args_amount1In", "args_amount0Out", "args_amount1Out", "blockHash"], axis=1)

        d.table(name).create_index("address")
        ea = EventArchive(
            d=d,
            table_name=name,
            filter_params={"topics": [swap_topic]},
            log_processor=try_process_log(e),
            post_processor=post_processor,
        )
    else:
        raise ValueError(name)

    d.con[name].create_index("transactionHash")
    d.con[name].create_index([("blockNumber", pymongo.ASCENDING)])# speed up searching
    return ea


def event_archive_parser() -> argparse.ArgumentParser:
    """A parser for common event archive run time parameters.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--stime", type=str, help="parsable by pd.to_datetime")
    parser.add_argument("--etime", type=str, default="20991231", help="parsable by pd.to_datetime")
    parser.add_argument("--fetch-new", action="store_true")

    return parser