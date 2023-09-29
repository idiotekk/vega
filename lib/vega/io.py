import os
import sys
import pandas as pd
from glob import glob
from pathlib import Path
from typing import Union, Sequence, Any, Tuple
from . import log


__all__ = [
    "load_json",
    "dump_json",
    "make_sure_parent_dir_exists",
]


def rel_path(this_file, *path: Tuple[str]) -> str:
    """ Return the abs path give the relative path to *this* file.
    """
    res = Path(this_file).parent.absolute()
    for _ in path:
        res = res / _
    return str(res)


def make_sure_parent_dir_exists(path: Union[str, Path]) -> Union[str, Path]:
    """ Create parent dir if not exists.
    """
    parent_dir = Path(path).parent
    if not parent_dir.exists():
        log.info(f"creating {parent_dir}")
        parent_dir.mkdir(parents=True, exist_ok=True)
    return path


def load_json(f_: Union[Path, str]) -> Any:
    import json
    log.info(f"reading {f_}")
    with open(str(f_), "r") as f:
        return json.load(f)


def dump_json(j: Union[list, dict], f_: Union[Path, str]):
    import json
    log.info(f"writing {f_}")
    f_ = make_sure_parent_dir_exists(f_)
    with open(str(f_), "w") as f:
        json.dump(j, f, indent=4)