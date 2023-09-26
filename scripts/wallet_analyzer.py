import os
import sys
sys.path.insert(0, "../lib")
import web3
from tqdm import tqdm
import vega
import pandas as pd
import numpy as np
from pprint import pprint
from pathlib import Path
from vega import log
from vega.evm.web3 import Web3Portal
from vega.evm.web3 import ContractEvent


def parse_trade(df):

    weth = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
    token0 = c.functions["token0"]().call()
    token1 = c.functions["token1"]().call()
    decimals0 = p.get_decimals(addr=token0)
    decimals1 = p.get_decimals(addr=token1)

    df["args_amount0"] = df["args_amount0In"] / (10**decimals0) - df["args_amount0Out"] / (10**decimals0)
    df["args_amount1"] = df["args_amount1In"] / (10**decimals1) - df["args_amount1Out"] / (10**decimals1)
    if token_addr == token0:
        df["token_amount"] = df["args_amount0"]
        df["weth_amount"] = df["args_amount1"]
    elif token_addr == token1:
        df["token_amount"] = df["args_amount1"]
        df["weth_amount"] = df["args_amount2"]
    else:
        raise ValueError(f"{token_addr} is not found in pair ({token0}, {token1})")
    df["side"] = np.where(df["token_amount"] < 1, 1, -1)
    df["timestamp_est"] = df["timestamp"].dt.tz_convert("US/Eastern")
    df["price_weth"] = -df["weth_amount"] / df["token_amount"] # remember negative sign
    df["trader"] = df["args_to"]


def addr_to_topic(addr):
    return addr.replace("0x", "0x" + "0"*24)


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("addr", type=str)
    parser.add_argument("--sdate", type=int)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    addr = args.addr
    file_path = os.path.expandvars(f"$HOME/vega/data/wallet/{addr}.csv")
    p = Web3Portal()
    p.init()
    addr = p.web3.to_checksum_address(addr)

    dummy_erc20_addr = "0x".ljust(42, "0")
    abi = p.get_abi(type="ERC20")
    c = p.web3.eth.contract(address=dummy_erc20_addr, abi=abi)
    e = c.events["Transfer"]()

    if args.restart or not os.path.exists(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
        stime = pd.to_datetime(str(args.sdate), utc=True)
        df0 = pd.DataFrame()
    else:
        df0 = pd.read_csv(file_path)
        stime = pd.to_datetime(df0["timestamp"].max())
        log.info(f"warm start at {stime}")

    etime = pd.Timestamp.utcnow()
    df = pd.concat([
        p.get_logs(
            stime=stime,
            etime=etime,
            log_processor=e.process_log,
            filter_params={"topics": e._get_event_filter_params(e.abi)["topics"] + extra_topics})
        for extra_topics in
        [
            [None, addr_to_topic(addr)],
            [addr_to_topic(addr), None],
        ]])
    print(df.describe())
    for i, row in tqdm(df.iterrows()):
        block_number = row["blockNumber"]
        balance = p.web3.eth.get_balance(addr, block_number)
        df.loc[i, "balance"] = balance
        df.loc[i, "balance_eth"] = balance / 1e18
    """
    all_txs = pd.DataFrame(
        p.scan.get(
        module="account",
        action="txlist",
        address=addr,
        startblock=sblock,
        endblock=eblock,
        page=1,
        offset=10,
        sort="asc",
    ))[["blockNumber", "transactionIndex", "hash"]]
    """
    if len(df) == 0:
        log.info("no new logs are found; exiting")
        exit(0)
    else:
        df = df.sort_values(["blockNumber", "transactionIndex"])
    output_vars = df.columns
    df_summary = df[output_vars].copy()
    df_total = pd.concat([
        df0,
        df_summary,
    ])
    
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    log.info(f"{file_path}")
    df_total.to_csv(file_path, index=False)