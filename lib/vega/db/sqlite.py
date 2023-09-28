import os
import sqlite3
import json
from . import log
import pandas as pd
import numpy as np
from typing import Union, List, Optional, Any
from pandas.api.types import is_string_dtype


__all__ = [
    "SQLiteDB",
]


class SQLiteDB:
    
    _con: sqlite3.Connection
   
    def connect(self, path: str, **kw):
        self._path = path
        self._con = sqlite3.connect(path, **kw)

    @property
    def con(self) -> sqlite3.Connection:
        return self._con
    
    def write(self,
              df: pd.DataFrame,
              *,
              table_name: str,
              index: Union[str, List[str]]):

        df = df.astype(str)
        if not self.table_exists(table_name):
            self.create_table(table_name=table_name, columns=list(df.columns), index=index)

        query = "REPLACE INTO {} ({}) VALUES ({}) ".format(table_name, ', '.join(df.columns), ', '.join(["?"]*len(df.columns)))
        for i in range(len(df)):
            self.execute(query, tuple(df.iloc[i]))
        self.con.commit()
        log.info(f"{len(df)} rows are written to {self._path}:{table_name}.")

    def create_table(self, *,
                     table_name: str,
                     columns: List[str],
                     index=Union[str, List[str]]):
        
        if isinstance(index, str):
            index = [index]
        assert all([_ in columns for _ in index]), f"not all of {index} are found in {columns}"
        assert not self.table_exists(table_name), f"table {table_name} already exists"
        query = f"""CREATE TABLE {table_name} ({",".join([f"{k} TEXT" for k in columns])},PRIMARY KEY ({",".join(index)}));"""
        self.execute(query)
        log.info(f"created table {table_name} at {self._path}; index = {index}")

    def read(self, query: str, parse_str_columns=True) -> pd.DataFrame:
        log.info(f"querying dataframe from {query}")
        df = pd.read_sql_query(query, self.con)
        if parse_str_columns is True:
            self.parse_str_columns(df, inplace=True)
        return df
    
    def read_table(self, table_name: str, parse_str_columns=True) -> pd.DataFrame:
        query = f"SELECT * from {table_name}"
        return self.read(query, parse_str_columns=parse_str_columns)

    def delete_table(self, table_name: str) -> bool:
        """ Return True if deleted is done.
        """
        while True:
            cmd = input(f"delete {table_name}? (yes/no)")
            if cmd == "yes":
                self.execute(f"DROP TABLE {table_name}")
                return True
            elif cmd == "no":
                return False

    def execute(self, query: str, *a):
        try:
            log.debug(f"executing query = {query}, args = {a}")
            return self.con.execute(query, *a)
        except Exception as e:
            log.error(f"{query} failed with error {e}")
            raise e
    
    def table_exists(self, table_name: str) -> bool:
        c = self.execute(f'''SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}' ''')
        return c.fetchone() is not None

    @staticmethod
    def parse_str_columns(df: pd.DataFrame, inplace: bool=True) -> Optional[pd.DataFrame]:
        """ Auto-parse string columns.

        Parameters
        ----------
        df : pd.DataFrame
        inplcace : bool
            If True, modify `df` in-place, return None. Otherwise,
            modify a copy of `df` and return the modified copy.
        """
        if inplace is not True:
            df = df.copy()
        for v in df.columns:
            if is_string_dtype(df[v]):
                if np.all(df[v].isin(["True", "False"])):
                    log.info(f"parsing boolean column {v}")
                    df[v] = np.where(df[v] == "True", True, False)
                elif np.all(df[v].str.isdigit()):
                    log.info(f"parsing integer column {v}")
                    df[v] = df[v].apply(lambda x: int(x))
                else:
                    pass
        if inplace is not True:
            return df