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
from vega.evm.web3 import Web3Portal, ERC20TokenTracker
from vega.evm.web3 import ContractEvent
from vega.evm.utils import lookup
from vega.apps.tables import TokenInfo, DATABASE_PATH
from vega.utils import apply_range
from typing import Callable


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--stime", type=str, help="parsable by pd.to_datetime")
    parser.add_argument("--etime", type=str, default="20991231", help="parsable by pd.to_datetime")
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()
    p = ERC20TokenTracker()
    p.init()
    token_info = TokenInfo()
    e = p.uniswap_v2_factory().events["PairCreated"]()

    WETH = lookup("addr")["WETH"]

    def download_pair_creation_logs(stime: pd.Timestamp, etime: pd.Timestamp) -> pd.DataFrame:

        df = p.get_logs(
            stime=stime,
            etime=etime,
            filter_params=e._get_event_filter_params(e.abi),
            log_processor=e.process_log,
        )
        if len(df) > 0:
            for addr in np.union1d(df["args_token0"].values, df["args_token1"].values):
                token_info.touch(addr)

    if args.bootstrap:
        stime = pd.to_datetime(token_info.db.read(" SELECT MAX(creationTime) from uniswap_v2_swap").iloc[0, 0])
        log.info(f"bootsrapping from {stime}")
    else:
        stime = pd.to_datetime(args.stime, utc=True)
    etime = min(pd.to_datetime(args.etime, utc=True), pd.Timestamp.utcnow())
    apply_range(
        func=download_pair_creation_logs,
        start=stime,
        end=etime,
        max_batch_size=pd.Timedelta("1h"))