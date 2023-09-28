import pandas as pd
from typing import Optional, Union, List, Callable, Dict, Sequence

__all__ = [
    "agg_df",
    "cross_join",
    "save_df",
]


def agg_df(df: pd.DataFrame,
           *,
           by: Union[str, List[str]],
           func: Callable,
           ) -> pd.DataFrame:
           
    return df.groupby(by).apply(lambda x: pd.Series(func(x))).reset_index()

    
def cross_join(**kw: Dict[str, Sequence]) -> pd.DataFrame:
    """ Equivalent to "CJ" in R data.table.
    """
    from functools import reduce
    df_list = []
    for key, value in kw.items():
        df_tmp = pd.DataFrame({key: list(value), "__key": 0})
        df_list.append(df_tmp)
    res = reduce(lambda x, y: pd.merge(x, y, on="__key"), df_list)
    return res.drop("__key", axis=1)


def save_df(df: pd.DataFrame,
            file: Union[str, Path],
            **kw) -> str:
    """ Write dataframe to csv, creating parent dir if not exists.
    """
    file = make_sure_parent_dir_exists(file)
    df.to_csv(file, **kw)
    log.info(f"df shape: {df.shape}, written to: {file}")
    return file