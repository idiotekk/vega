import os
import sys
sys.path.insert(0, "../lib")
import web3
import vega
import pandas as pd
import numpy as np
from pprint import pprint
from pathlib import Path
from vega import log
from vega.evm.web3 import Web3Portal
from vega.evm.web3 import ContractEvent
from vega.db.sqlite import SQLiteDB
from vega.utils import apply_range
from typing import Callable


def get_uniswap_v2_swap_event(p: Web3Portal) -> ContractEvent:

    token_addr = "0x840768f4467BEc882921EBae9F5f621fDc2C9E97"
    weth_addr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    uniswap_v2_factory = p.get_contract(addr="0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f")
    pool = uniswap_v2_factory.functions["getPair"](weth_addr, token_addr).call()
    abi = p.get_abi(addr=pool)
    c = p.web3.eth.contract(address=pool, abi=abi)
    e = c.events["Swap"]()
    return e



if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--stime", type=str, help="parsable by pd.to_datetime")
    parser.add_argument("--etime", type=str, default="20991231", help="parsable by pd.to_datetime")
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()
    p = Web3Portal()
    p.init()
    e = get_uniswap_v2_swap_event(p)

    db = SQLiteDB()
    db.connect(os.path.expandvars(f"$HOME/vega/data/dex.db"))
    table_name = "uniswap_v2_swap"
    index_cols = ["blockNumber", "logIndex"]

    def download_swap_logs(stime: pd.Timestamp, etime: pd.Timestamp) -> pd.DataFrame:

        df = p.get_logs(
            stime=stime,
            etime=etime,
            filter_params={
                "topics": e._get_event_filter_params(e.abi)["topics"][:1]
            },
            log_processor=e.process_log,
        )
        df["amount0"] = df["args_amount0In"] - df["args_amount0Out"]
        df["amount1"] = df["args_amount1In"] - df["args_amount1Out"]
        df = df.drop(["args_amount0In", "args_amount1In", "args_amount0Out", "args_amount1Out", "blockHash"], axis=1)
        if len(df) == 0:
            return
        else:
            db.write(df, table_name=table_name, index=index_cols)

    if args.bootstrap:
        stime = pd.to_datetime(db.read(" SELECT MAX(timestamp) from uniswap_v2_swap").iloc[0, 0])
        log.info(f"bootsrapping from {stime}")
    else:
        stime = pd.to_datetime(args.stime, utc=True)
    etime = min(pd.to_datetime(args.etime, utc=True), pd.Timestamp.utcnow())
    apply_range(
        func=download_swap_logs,
        start=stime,
        end=etime,
        max_batch_size=pd.Timedelta("1h"))