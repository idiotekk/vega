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
    args = parser.parse_args()
    p = Web3Portal()
    p.init()
    e = get_uniswap_v2_swap_event(p)

    file_path = os.path.expandvars(f"$HOME/vega/data/test/dex_trades.csv")
    etime = pd.Timestamp.utcnow()
    stime = etime - pd.Timedelta("30min")
    df = p.get_logs(
        stime=stime,
        etime=etime,
        filter_params={
            "topics": e._get_event_filter_params(e.abi)["topics"][:1]
        },
        log_processor=e.process_log,
    )
    if len(df) == 0:
        log.info("no new logs are found; exiting")
        exit(0)
    df["trader"] = df["args_to"]

    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    log.info(f"{file_path}")
    df.to_csv(file_path, index=False)