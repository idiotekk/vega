import os
import pymongo
from tqdm import tqdm
import pandas as pd
from pymongo.database import Database as MongoDataBase # for hint
from pymongo import MongoClient, errors
from pymongo.server_api import ServerApi
from typing import Union, List

from . import DataBase, log


class MongoDB(DataBase):

    _db = MongoDataBase

    def init(self, db_name: str="dex"):
        user = os.getenv("MONGODB_USERNAME")
        password = os.getenv("MONGODB_PASSWORD")
        uri = f"mongodb+srv://{user}:{password}@vega.konghml.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        self._db = client[db_name]
        log.info(f"connected to mongodb database {db_name}")
    
    def table(self, table_name):
        return self.db[table_name]

    @property
    def db(self) -> MongoDataBase:
        return self._db

    def write(self, df: pd.DataFrame, *,
              table_name: str,
              index: Union[str, List[str]],
              update: bool=False,
              ):
        if isinstance(index, str):
            index = [index]
        if not self.table_exists(table_name=table_name):
            self.create_table(table_name=table_name, index=index)
        if '_id' in df:
            df = df.drop("_id", axis=1)
        df = df.astype(str) # get around the 8-bit int # chain data is mostly int256 or string anyways

        if update:
            insert_count = 0
            update_count = 0
            for _, row in tqdm(df.iterrows()):
                try:
                    self.db[table_name].insert_one(row.to_dict())
                    insert_count += 1
                except errors.DuplicateKeyError as e:
                    d1 = {k: v for k, v in row.to_dict().items() if k in index}
                    d2 = {k: v for k, v in row.to_dict().items() if k not in index}
                    self.db[table_name].update_one(d1, {"$set": d2})
                    update_count += 1
            log.info(f"inserted: {insert_count}, updated: {update_count} to {table_name}")
        else:
            from pymongo.write_concern import WriteConcern
            self.db[table_name].with_options(write_concern=WriteConcern(w=0)).insert_many([
                row.to_dict() for _, row in df.iterrows()
            ], ordered=False)
            log.info(f"inserted: {len(df)}, to {table_name} (ignored duplicates)")


    def create_table(self, *,
                     table_name: str,
                     columns: List[str]=[],
                     index: Union[str, List[str]],
                     ):

        if isinstance(index, str):
            index = [index]
        if columns:
            log.warning(f"MongoDB disgarding columns {columns}.")
        table = self._db[table_name]
        table.create_index([(_, pymongo.ASCENDING) for _ in index], unique=True)
        log.info(f"created {table_name} with index={index}")

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.db.list_collection_names()

    def delete_table(self, table_name: str) -> bool:
        while True:
            cmd = input(f"delete {table_name}? (yes/no)")
            if cmd == "yes":
                self.db.drop_collection(table_name)
                return True
            elif cmd == "no":
                return False
    
    def read_table(self, table_name, *a, **kw):
        return pd.DataFrame.from_records(
            [dict(i) for i in self.db[table_name].find(*a, **kw)]
        )