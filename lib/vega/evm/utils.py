import typing
from hexbytes import HexBytes
import pandas as pd
from functools import lru_cache

from ..io import rel_path, load_json


__all__ = [
    "flatten_dict",
    "utcnow",
    "to_utc",
    "to_int",
    "lookup",
    "addr_to_topic",
]


def flatten_dict(d: dict, sep: str="_") -> dict:
    """
    Flatten a nested dictionary.

    Examples:
        flatten_dict({"a": 1, "b": {"c": 2, "d": 3,}})
    """
    def _flatten_dict_helper( # i really hate this function name
        d: typing.Any,
        sep: str="_") -> typing.Any:

        if isinstance(d, HexBytes):
            return d.hex()
        elif hasattr(d, "items"):
            d_ = {}
            for k, v in d.items():
                assert isinstance(k, str), f"key can only be str, got {k} with type {type(k)}"
                if hasattr(v, "items"):
                    for kk, vv in v.items():
                        d_[f"{k}{sep}{kk}"] = _flatten_dict_helper(vv)
                else:
                    d_[k] = _flatten_dict_helper(v)
            return d_
        else:
            return d

    return _flatten_dict_helper(d)


def utcnow() -> pd.Timestamp:
    return pd.Timestamp.utcnow()


def to_utc(t: pd.Timestamp) -> pd.Timestamp:
    if t.tzinfo is not None:
        return t.tz_convert("UTC")
    else:
        raise ValueError("can't convert tz-naive timetstamp to UTC!")


def to_int(t: pd.Timestamp, unit="ns") -> int:
    """ Convert timestamp to integer.
    `t` is required to be tz-aware.
    """
    ns = int(to_utc(t).value)
    if unit == "ns":
        return ns
    elif unit == "s":
        return ns // int(1e9)
    else:
        raise ValueError(f"unsupported unit {unit}")


@lru_cache(maxsize=None)
def lookup(*path):
    """Get json object under ./const/ .
    """
    return load_json(rel_path(__file__, "const", *path) + ".json")


@lru_cache(maxsize=None)
def addr_to_topic(addr):
    return addr.replace("0x", "0x" + "0"*24)