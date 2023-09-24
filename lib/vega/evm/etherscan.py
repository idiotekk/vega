import os
import requests
import time
from typing import Any
from . import log


__all__ = [
    "Etherscan",
    "Etherscanner",
]


class ResponseParser:

    @staticmethod
    def parse(response: requests.Response) -> Any:
        content = response.json()
        result = content["result"]
        if "status" in content.keys():
            status = bool(int(content["status"]))
            message = content["message"]
            assert status, f"{result} -- {message}"
        else:
            raise ValueError(f"failed to get status from response {content}")
        return result


class Etherscan:
    
    _api_key: str
    _base_url: str
    _retry_wait_seconds: float = 1.001 # retry after this seconds
    _max_retries: int = 5

    def __init__(self) -> None:
        api_key_env_var = "ETHERSCAN_API_KEY"
        self._api_key = os.environ[api_key_env_var]
        self._base_url = "https://api.etherscan.io/api?"
        
    def get(self, **kw):
        kw["apikey"] = self._api_key
        url = self._base_url + "&".join([f"{k}={v}" for k, v in kw.items()])

        retries = 0
        while retries < self._max_retries:
            try:
                r = requests.get(url, headers={"User-Agent": ""})
                return ResponseParser.parse(r)
            except Exception as e:
                print(f"{url} failed with error:\n{e}")
                print(f"waiting for {self._retry_wait_seconds} seconds...")
                time.sleep(self._retry_wait_seconds)
                retries += 1
    
    def get_block_number_by_timestamp(self, timestamp: int) -> int:
        kw = dict(
            module = "block",
            action = "getblocknobytime",
            timestamp = timestamp,
            closest = "before",
            apikey = self._api_key,
        )
        return int(self.get(**kw))