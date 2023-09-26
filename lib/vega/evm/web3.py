import os
import typing
import json
from web3 import Web3
from web3.contract.contract import ContractEvent # for typing
from functools import cache
import pandas as pd

from . import log
from .etherscan import Etherscan
from .utils import to_int



class Web3Portal:

    _web3: Web3
    _scan: Etherscan

    def init(self):
        """ Connect to mainnet mainnet via infura (let's not be too generic).
        """

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
        processed_logs = [flatten_dict(dict(log_processor(raw_log))) for raw_log in raw_logs]
        df = pd.DataFrame(processed_logs)
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
    
    def get_block_number_by_timestamp(self, ts: pd.Timestamp) -> int:
        if isinstance(ts, pd.Timestamp):
            return self.scan.get_block_number_by_timestamp(to_int(ts, "s"))
        else:
            raise TypeError(f"unsupported input type {ts} type = {type(ts)}")

    @cache
    def get_decimals(self, addr: str) -> int:
        """ Get the decimals of an ERC20 token.
        """
        c = self.web3.eth.contract(address=addr, abi=self.get_abi(type="ERC20"))
        return c.functions["decimals"]().call()

    @cache
    def get_abi(self, *, addr: typing.Optional[str]=None, type: typing.Optional[str]=None) -> list:
        """ Get abi from contract address.
        """
        _ABI_MAP = {
            "ERC20": """[{"constant": true, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "payable": false, "stateMutability": "view", "type": "function"}, {"constant": false, "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "payable": false, "stateMutability": "nonpayable", "type": "function"}, {"constant": true, "inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "payable": false, "stateMutability": "view", "type": "function"}, {"constant": false, "inputs": [{"name": "_from", "type": "address"}, {"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transferFrom", "outputs": [{"name": "", "type": "bool"}], "payable": false, "stateMutability": "nonpayable", "type": "function"}, {"constant": true, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": false, "stateMutability": "view", "type": "function"}, {"constant": true, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "payable": false, "stateMutability": "view", "type": "function"}, {"constant": true, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "payable": false, "stateMutability": "view", "type": "function"}, {"constant": false, "inputs": [{"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transfer", "outputs": [{"name": "", "type": "bool"}], "payable": false, "stateMutability": "nonpayable", "type": "function"}, {"constant": true, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "payable": false, "stateMutability": "view", "type": "function"}, {"payable": true, "stateMutability": "payable", "type": "fallback"}, {"anonymous": false, "inputs": [{"indexed": true, "name": "owner", "type": "address"}, {"indexed": true, "name": "spender", "type": "address"}, {"indexed": false, "name": "value", "type": "uint256"}], "name": "Approval", "type": "event"}, {"anonymous": false, "inputs": [{"indexed": true, "name": "from", "type": "address"}, {"indexed": true, "name": "to", "type": "address"}, {"indexed": false, "name": "value", "type": "uint256"}], "name": "Transfer", "type": "event"}]"""
        }
        if addr is not None:
            return self.scan.get(module="contract", action="getabi", address=addr)
        elif type is not None:
            assert type in _ABI_MAP.keys(), f"{type} not found in {list(_ABI_MAP.keys())}"
            return json.loads(_ABI_MAP[type])

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

    def get_contract(self, *, addr: str, type: typing.Optional[str]=None):

        if type is None:
            abi = self.get_abi(addr=addr)
        else:
            abi = self.get_abi(type=type)
        contract = self.web3.eth.contract(address=addr, abi=abi)
        return contract