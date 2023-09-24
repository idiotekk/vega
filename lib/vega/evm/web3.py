import os
import typing
from web3 import Web3
from web3.contract.contract import ContractEvent # for typing
from functools import cache
import pandas as pd

from . import log
from .etherscan import Etherscan
from .utils import to_int


class EventListener:

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
        from .utils import flatten_dict
        processed_logs = [flatten_dict(dict(log_processor(raw_log))) for raw_log in raw_logs]
        df = pd.DataFrame(processed_logs)
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
    
    def get_block_number_from_timestamp(self, ts: pd.Timestamp):
        if isinstance(pd.Timestamp):
            return self.scan.get_block_number_by_timestamp(to_int(ts, "s"))
        else:
            raise TypeError(f"unsupported input type {ts} type = {type(ts)}")

    @cache
    def get_abi(self, addr: str) -> list:
        """ Get abi from contract address.
        """
        return self.scan.get(module="contract", action="getabi", address=addr)