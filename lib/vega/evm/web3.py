import os
import typing
import json
from web3 import Web3
from web3.contract.contract import ContractEvent, Contract # for typing
from functools import lru_cache
import pandas as pd

from ..utils import Singleton
from . import log
from .etherscan import *
from .utils import *



class Web3Portal(Etherscanner, metaclass=Singleton):

    _web3: Web3

    def __init__(self):
        self.init()

    def init(self):
        """ Connect to mainnet mainnet via infura (let's not be too generic).
        """
        if hasattr(self, "_web3") and self._web3.is_connected():
            pass
        else:
            base_url = "https://mainnet.infura.io/v3"
            api_key = os.environ["INFURA_API_KEY"]
            url = f"{base_url}/{api_key}"
            self._web3 = Web3(Web3.HTTPProvider(url))
            log.info(f"connecting to: {url}")
            assert self._web3.is_connected()

    @property
    def web3(self):
        return self._web3

    def get_logs(self, *,
                 sblock: typing.Optional[int],
                 eblock: typing.Optional[int],
                 stime: typing.Optional[pd.Timestamp],
                 etime: typing.Optional[pd.Timestamp],
                 filter_params: dict,
                 log_processor: typing.Callable,
                 parse_timestamp: bool=False,
                 ) -> typing.List[typing.Dict[str, str]]:
        """
        Filter logs by `filter_params` between [stime, etime).
        """

        if stime is None:
            sblock = self.get_block_number_by_timestamp(stime)
        if eblock is None:
            eblock = self.get_block_number_by_timestamp(etime)
        _filter_params = {
            "fromBlock": sblock,
            "toBlock": eblock,
            **filter_params
        }

        log.info(f"filtering logs {_filter_params}. (number of blocks: {eblock - sblock + 1})")
        raw_logs = self.web3.eth.get_logs(_filter_params)
        log.info(f"number of logs: {len(raw_logs)}")
        if len(raw_logs) == 0:
            return {}
        from .utils import flatten_dict
        processed_logs = [log_processor(raw_log) for raw_log in raw_logs]
        processed_logs = [flatten_dict(_) for _ in processed_logs if _] # a backdoor to allow log_processor to give up if can't parse
        return processed_logs

    def get_event_logs(self, *,
                       stime: pd.Timestamp,
                       etime: pd.Timestamp,
                       event: ContractEvent,
                       ) -> pd.DataFrame:
        """
        Get logs for an event between [stime, etime).
        """
        return self.get_logs(
            stime=stime,
            etime=etime,
            filter_params=event._get_event_filter_params(event.abi),
            log_processor=event.process_log)
    
    @lru_cache(maxsize=None)
    def get_block_number_by_timestamp(self, ts: pd.Timestamp) -> int:
        if isinstance(ts, pd.Timestamp):
            return self.scan.get_block_number_by_timestamp(to_int(ts, "s"))
        else:
            raise TypeError(f"unsupported input type {ts} type = {type(ts)}")


    @lru_cache(maxsize=None)
    def get_abi(self, *, addr: typing.Optional[str]=None, type: typing.Optional[str]=None) -> list:
        """ Get abi from contract address.
        """
        if addr is not None:
            return self.scan.get(module="contract", action="getabi", address=addr)
        elif type is not None:
            abi = const("abi/erc20")
            return abi

    def get_timestamp_from_block_number(self, block_number: typing.Union[pd.Series, int]) -> typing.Union[pd.Series, int]:

        def block_number_to_ts(bn: int) -> pd.Timestamp:
            return pd.to_datetime(self.web3.eth.get_block(bn).timestamp * 1e9, utc=True)

        if isinstance(block_number, int):
            return block_number_to_ts(block_number)
        else:
            block_number = np.array(block_number)
            min_block = int(block_number.min())
            max_block = int(block_number.max())
            stime = block_number_to_ts(min_block)
            etime = block_number_to_ts(max_block)
            if min_block == max_block:
                return stime
            else:
                return (etime - stime) / (max_block - min_block) * (block_number - min_block) + stime

    @lru_cache(maxsize=None)
    def get_contract(self, *,
                     addr: typing.Optional[str],
                     type: typing.Optional[str]=None,
                     ) -> Contract:

        abi = self.get_abi(addr=addr, type=type)
        contract = self.web3.eth.contract(address=addr, abi=abi)
        log.info(f"constructed contract {addr}")
        return contract


class ERC20TokenTracker(Web3Portal):

    @lru_cache(maxsize=None)
    def get_uniswap_v2_factory(self):
        return self.get_contract(addr=const("addr")["UniswapV2Factory"])

    @lru_cache(maxsize=None)
    def get_uniswap_v2_pair(self, addr) -> str:
        pair_addr = self.get_uniswap_v2_factory().functions["getPair"](const("addr")["WETH"], addr).call()
        return self.get_contract(addr=pair_addr)

    @lru_cache(maxsize=None)
    def get_creation_tx(self, addr: str) -> dict:
        log_ = self.scan.get(
            module="contract",
            action="getcontractcreation",
            contractaddresses=addr,
        )[0]
        return self.web3.eth.get_transaction(log_["txHash"])

    def gather_token_info(self, addr: str) -> dict:

        addr = csaddr(addr)
        c = self.get_contract(addr=addr, type="erc20")
        token_info = {
            "addr": addr,
        }
        for property_name in ["name", "symbol", "totalSupply", "decimals"]:
            try:
                value = c.functions[property_name]().call()
                token_info[property_name] = value
            except Exception as e:
                log.error(f"failed to get {property_name} for {addr}, {e}")
                token_info[property_name] = ""


        creation_tx = self.get_creation_tx(addr)
        creation_blkno = creation_tx["blockNumber"]
        token_info["creationBlockNumber"] = creation_blkno
        token_info["creationTime"] = self.get_timestamp_from_block_number(block_number=creation_blkno)
        token_info["deployer"] = creation_tx["from"]

        optional_fields = [
            "WETHPoolV2",
            "WETHPoolV2CreationTime",
            "WETHPoolV2Token0",
            "WETHPoolV2Token1",
        ]
        for f_ in optional_fields:
            token_info[f_] = ""
        try:
            pool_contract = self.get_uniswap_v2_pair(addr)
            token_info["WETHPoolV2"] = pool_contract.address
            pool_creation_tx = self.get_creation_tx(token_info["WETHPoolV2"])
            token_info["WETHPoolV2CreationTime"] = self.get_timestamp_from_block_number(block_number=int(pool_creation_tx["blockNumber"]))
            token_info["WETHPoolV2Token0"] = pool_contract.functions["token0"]().call()
            token_info["WETHPoolV2Token1"] = pool_contract.functions["token1"]().call()
        except Exception as e:
            log.info(f"failed to find WETH Pool V2: {e}")

        return token_info

    @lru_cache(maxsize=None)
    def get_decimals(self, addr: str) -> int:
        """ Get the decimals of an ERC20 token.
        """
        c = self.web3.eth.contract(address=addr, abi=self.get_abi(type="ERC20"))
        return c.functions["decimals"]().call()