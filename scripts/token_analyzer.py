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


class ERC20TokenTracker(Web3Portal):

    def get_univswap_v2_pair(self, addr) -> str:
        c = self.get_contract(addr="0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f")
        return c.functions["getPair"]("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", addr).call()

    def get_token_creation_time(self, addr: str) -> pd.Timestamp:

        c = self.get_contract(addr=addr)
        creation_log = self.get_logs(
            stime=pd.to_datetime("20180101").tz_localize("UTC"),
            etime=pd.Timestamp.utcnow(),
            filter_params=dict(
                address=addr,
                topics=[
                    #"0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                ]),
            log_processor=lambda x:x
        ).iloc[0].to_dict()
        creation_block_number = int(creation_log["blockNumber"])
        creation_time = self.get_timestamp_from_block_number(block_number=creation_block_number)
        log.info(f"contract {addr} was created at block = {creation_block_number}, {creation_time}")
        return creation_time


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("addr", type=str)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    #log.getLogger().setLevel(log.DEBUG)

    #token_addr = "0x1e8ee2fa31bfe35451c1310130029dd37695c23b"
    token_addr = args.addr
    file_path = os.path.expandvars(f"$HOME/vega/data/wallet/{token_addr}.csv")
    p = ERC20TokenTracker()
    p.init()
    token_addr = p.web3.to_checksum_address(token_addr)
    pool = p.get_univswap_v2_pair(addr=token_addr)
    abi = p.get_abi(addr=pool)

    c = p.web3.eth.contract(address=pool, abi=abi)
    e = c.events["Swap"]()

    if args.restart or not os.path.exists(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
        stime = p.get_token_creation_time(addr=token_addr)
        df0 = pd.DataFrame()
    else:
        df0 = pd.read_csv(file_path)
        stime = pd.to_datetime(df0["timestamp"].max())
        log.info(f"warm start at {stime}")

    etime = pd.Timestamp.utcnow()
    df = p.get_event_logs(stime=stime, etime=etime, event=e)
    if len(df) == 0:
        log.info("no new logs are found; exiting")
        exit(0)
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

    if args.debug is True:
        output_vars = df.columns
    else:
        output_vars = ["trader", "token_amount", "weth_amount", "side", "price_weth", "timestamp", "timestamp_est", "transactionHash"]

    df_summary = df[output_vars].copy()
    df_total = pd.concat([
        df0,
        df_summary,
    ])
    
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    log.info(f"{file_path}")
    df_total.to_csv(file_path, index=False)