import os
import typing
import json
from web3 import Web3
from web3.contract.contract import ContractEvent, Contract # for typing
from functools import lru_cache
import pandas as pd

from . import log
from .etherscan import Etherscan
from .utils import to_int, lookup, addr_to_topic



class Web3Portal:

    _web3: Web3 = None
    _scan: Etherscan
    _instance = None

    def __new__(cls, *args, **kwargs):
        """ Singleton.
        """
        if not cls._instance:
            cls._instance = super(Web3Portal, cls).__new__(
                                cls, *args, **kwargs)
            log.info(f"created {cls} instance.")
        return cls._instance

    def init(self):
        """ Connect to mainnet mainnet via infura (let's not be too generic).
        """
        if self._web3 is not None and self._web3.is_connected():
            pass
        else:
            base_url = "https://mainnet.infura.io/v3"
            api_key = os.environ["INFURA_API_KEY"]
            url = f"{base_url}/{api_key}"
            self._web3 = Web3(Web3.HTTPProvider(url))
            log.info(f"connecting to: {url}")
            assert self._web3.is_connected()

        self._scan = Etherscan()

    @property
    def scan(self):
        return self._scan

    @property
    def web3(self):
        return self._web3

    def get_logs(self, *,
                 stime: pd.Timestamp,
                 etime: pd.Timestamp,
                 filter_params: dict,
                 log_processor: typing.Callable,
                 parse_timestamp: bool=False,
                 ) -> pd.DataFrame:
        """
        Filter logs by `filter_params` between [stime, etime).
        """

        from_block = self.get_block_number_by_timestamp(stime)
        to_block = self.get_block_number_by_timestamp(etime) - 1
        _filter_params = {
            "fromBlock": from_block,
            "toBlock": to_block,
        }
        _filter_params.update(filter_params)
            
        log.info(f"filtering logs {_filter_params}. (number of blocks: {to_block - from_block + 1})")
        raw_logs = self.web3.eth.get_logs(_filter_params)
        log.info(f"number of logs: {len(raw_logs)}")
        if len(raw_logs) == 0:
            return pd.DataFrame()
        from .utils import flatten_dict
        processed_logs = [log_processor(raw_log) for raw_log in raw_logs]
        processed_logs = [flatten_dict(_) for _ in processed_logs if _] # a backdoor to allow log_processor to give up if can't parse
        df = pd.DataFrame(processed_logs)
        if parse_timestamp:
            df["timestamp"] = self.get_timestamp_from_block_number(df["blockNumber"])
        return df

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
    def get_decimals(self, addr: str) -> int:
        """ Get the decimals of an ERC20 token.
        """
        c = self.web3.eth.contract(address=addr, abi=self.get_abi(type="ERC20"))
        return c.functions["decimals"]().call()

    @lru_cache(maxsize=None)
    def get_abi(self, *, addr: typing.Optional[str]=None, type: typing.Optional[str]=None) -> list:
        """ Get abi from contract address.
        """
        if addr is not None:
            return self.scan.get(module="contract", action="getabi", address=addr)
        elif type is not None:
            from ..io import load_json, rel_path
            abi = lookup("abi/erc20")
            return abi

    def get_timestamp_from_block_number(self, block_number: typing.Union[pd.Series, int]) -> typing.Union[pd.Series, int]:

        def block_number_to_ts(bn: int) -> pd.Timestamp:
            return pd.to_datetime(self.web3.eth.get_block(bn).timestamp * 1e9, utc=True)

        if isinstance(block_number, int):
            return block_number_to_ts(block_number)
        else:
            min_block = int(block_number.min())
            max_block = int(block_number.max())
            stime = block_number_to_ts(min_block)
            etime = block_number_to_ts(max_block)
            if min_block == max_block:
                return stime
            else:
                return (etime - stime) / (max_block - min_block) * (block_number - min_block) + stime

    @lru_cache(maxsize=None)
    def get_contract(self, *, addr: str, type: typing.Optional[str]=None) -> Contract:

        if type is None:
            abi = self.get_abi(addr=addr)
        else:
            abi = self.get_abi(type=type)
        contract = self.web3.eth.contract(address=addr, abi=abi)
        log.info(f"constructed contract {addr}")
        return contract


class ERC20TokenTracker(Web3Portal):

    @lru_cache(maxsize=None)
    def uniswap_v2_factory(self):
        return self.get_contract(addr=lookup("addr")["UniswapV2Factory"])

    @lru_cache(maxsize=None)
    def get_univswap_v2_pair(self, addr) -> str:
        return self.uniswap_v2_factory().functions["getPair"](lookup("addr")["WETH"], addr).call()

    @lru_cache(maxsize=None)
    def get_token_creation_log(self, addr: str) -> dict:
        creation_log = self.get_logs(
            stime=pd.to_datetime("20180101").tz_localize("UTC"),
            etime=pd.Timestamp.utcnow(),
            filter_params=dict(
                address=addr,
                topics=[
                    lookup("topic")["ContractCreation"],
                    addr_to_topic(lookup("addr")["NULL"]),
                ]),
            log_processor=lambda x:x,
            parse_timestamp=False,
        )
        assert len(creation_log) > 0, "can't find token creation event"
        return creation_log.iloc[0].to_dict()

    def gather_token_info(self, addr: str) -> dict:

        c = self.get_contract(addr=addr, type="erc20")
        token_info = {
            "addr": addr,
        }
        for property_name in [_["name"] for _ in c.abi if _["type"] == "function" and _["stateMutability"] == "view" and not _["inputs"]]:
            try:
                token_info[property_name] = c.functions[property_name]().call()
            except Exception as e:
                log.info(f"failed to get {property_name} for {addr}, {e}")
                token_info[property_name] = ""

        # i hate so many try excepts but deployers just don't follow standards
        # try to get these fields
        optional_fields = [
            "creationBlockNumber",
            "creationTime",
            "deployer",
            "WETHPoolV2",
            "WETHPoolV2CreationTime",
            "WETHPoolV2Token0",
            "WETHPoolV2Token1",
        ]
        for f_ in optional_fields:
            token_info[f_] = ""
        try:
            creation_log = self.get_token_creation_log(addr)
            if creation_log:
                token_info["creationBlockNumber"] = int(creation_log["blockNumber"])
                token_info["creationTime"] = self.get_timestamp_from_block_number(block_number=int(creation_log["blockNumber"]))
                token_info["deployer"] = self.web3.eth.get_transaction(creation_log["transactionHash"])["from"]
        except Exception as e:
            log.info(f"failed to get creation event. error: {e}")

        try:
            token_info["WETHPoolV2"] = self.get_univswap_v2_pair(addr)
            pool_contract = self.get_contract(addr=token_info["WETHPoolV2"])
            pool_creation_log = self.get_token_creation_log(token_info["WETHPoolV2"])
            token_info["WETHPoolV2CreationTime"] = self.get_timestamp_from_block_number(block_number=int(pool_creation_log["blockNumber"]))
            token_info["WETHPoolV2Token0"] = pool_contract.functions["token0"]().call()
            token_info["WETHPoolV2Token1"] = pool_contract.functions["token1"]().call()
        except Exception as e:
            log.info(f"failed to find WETH Pool V2. Error: {e}")

        return token_info