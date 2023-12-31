import pandas as pd
from pathlib import Path

from . import log


class CSVDataBase(DataBase):

    def init(self, root_dir):
        self._root_dir = root_dir

    def read_table(self, path):
        abs_path = (Path(self.root_dir) / path).absolute()
        if not abs_path.exists():
            raise FileNotFoundError(abs_path)
        else:
            log.info(f"reading {abs_path}")
            return pd.read_csv(abs_path)

    @property
    def root_dir(self):
        return self._root_dir