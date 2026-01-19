#%%

from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any
from pymongo.cursor import Cursor
from collections import defaultdict
from utils.doc_schema_ext import DataSchemaUtils, DataSchemaModel

# %%



class CollectionLevelSchemaInfoModel:
    def __init__(
        self,
        db_nm: str,
        col_nm: str,
        collection_ns: str,
        document_data_size: int,
        n_documents: int,
        avg_document_data_size: float,
        document_storage_size: int,
        index_details: Dict[str, Any],
        fetch_datetime: datetime
    ):
        self.db_nm = db_nm
        self.col_nm = col_nm
        self.collection_ns = collection_ns
        self.document_data_size = document_data_size
        self.n_documents = n_documents
        self.avg_document_data_size = avg_document_data_size
        self.document_storage_size = document_storage_size
        self.index_details = index_details
        self.fetch_datetime = fetch_datetime

    def to_dict(self):
        return {
            'db_nm': self.db_nm,
            'col_nm': self.col_nm,
            'collection_ns': self.collection_ns,
            'document_data_size': self.document_data_size,
            'n_documents': self.n_documents,
            'avg_document_data_size': self.avg_document_data_size,
            'document_storage_size': self.document_storage_size,
            'index_details': self.index_details,
            'fetch_datetime': self.fetch_datetime
        }

class CollectionSchemaModel:

    def __init__(
        self, db_nm: str, col_nm: str, mongo_client: MongoClient, 
        n_doc_to_derive: int = None,
        fetch_doc_batch_size: int = 1000
    ):

        self.db_nm = db_nm
        self.col_nm = col_nm
        self.n_doc_to_derive = n_doc_to_derive
        self.fetch_doc_batch_size = fetch_doc_batch_size

        self.get_schema(mongo_client, batch_size=fetch_doc_batch_size)
    
    def get_schema(self, mongo_client: MongoClient, batch_size: int = 1000):

        col_stats = mongo_client[self.db_nm].command("collstats", self.col_nm)

        self.collection_schema_info = CollectionLevelSchemaInfoModel(
            db_nm = self.db_nm,
            col_nm = self.col_nm,
            collection_ns = col_stats["ns"],
            document_data_size = col_stats["size"],
            n_documents = col_stats["count"],
            avg_document_data_size = col_stats.get("avgObjSize", None),
            document_storage_size = col_stats["storageSize"],
            index_details = {
                'ttl_index_size': col_stats.get("totalIndexSize", None),
                'index_sizes': col_stats.get("indexSizes", None),
                'settings': mongo_client[self.db_nm][self.col_nm].index_information()
            },
            fetch_datetime = datetime.now(ZoneInfo("Asia/Hong_Kong"))
        )


        if self.n_doc_to_derive is not None:
            self.n_doc_to_derive = min(self.n_doc_to_derive, self.collection_schema_info.n_documents)
        else:
            self.n_doc_to_derive = self.collection_schema_info.n_documents

        collect_client = mongo_client[self.db_nm][self.col_nm]
        cursor = collect_client.find({}).limit(self.n_doc_to_derive)
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
            'collection_schema_info': self.collection_schema_info,
            'doc_schema_details': self.doc_schema_details
        }