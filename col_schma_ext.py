#%%

from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict
from pymongo.cursor import Cursor
from collections import defaultdict
from doc_schema_ext import DataSchemaUtils, DataSchemaModel
# %%

class CollectionSchemaModel:

    def __init__(self, db_nm: str, col_nm: str, mongo_client: MongoClient):

        self.db_nm = db_nm
        self.col_nm = col_nm

        self.get_schema(mongo_client)
    
    def get_schema(self, mongo_client: MongoClient, batch_size: int = 1000):

        col_stats = mongo_client[self.db_nm].command("collstats", self.col_nm)

        self.collection_ns = col_stats["ns"]
        self.document_data_size = col_stats["size"]
        self.n_documents = col_stats["count"]
        self.avg_document_data_size = col_stats["avgObjSize"]
        self.document_storage_size = col_stats["storageSize"]
        self.fetch_datetime = datetime.now(ZoneInfo("Asia/Hong_Kong"))
        self.index_details = {
            'ttl_index_size': col_stats.get("totalIndexSize", None),
            'index_sizes': col_stats.get("indexSizes", None),
            'settings': mongo_client[self.db_nm][self.col_nm].index_information()
        }


        collect_client = mongo_client[self.db_nm][self.col_nm]
        cursor = collect_client.find({})
        derived_doc_schema_results = None
        while True:
            docs = CollectionSchemaModel.fetch_many_by_cursor(cursor, batch_size=batch_size)
            if docs is None: break
            
            derived_doc_schema_results = DataSchemaUtils.derive_schema(docs, agg_derived_schema=derived_doc_schema_results)
            
        self.doc_schema_details: defaultdict[str, DataSchemaModel] = derived_doc_schema_results

    @staticmethod
    def fetch_many_by_cursor(cursor: Cursor, batch_size: int) -> List[Dict]:
        try:
            n_fetch = 1
            err, res = None, []
            while n_fetch <= batch_size:
                res.append(cursor.next())
                n_fetch += 1
                
        except StopIteration:
            pass
        
        except Exception as e:
            err = e
        
        finally:
            if err is not None: raise err
            return res if len(res) > 0 else None


    def to_dict(self):
        return {
            'collection_ns': self.collection_ns,
            'database_nm': self.db_nm,
            'collection_nm': self.col_nm,
            'collection_size': self.collection_size,
            'document_count': self.document_count,
            'avg_document_size': self.avg_document_size,
            'storage_size': self.storage_size,
            'index_details': self.index_details,
            'doc_schema_details': self.doc_schema_details
        }