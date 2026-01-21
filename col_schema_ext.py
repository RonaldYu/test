from pymongo import MongoClient, DESCENDING
from pymongo.cursor import Cursor
from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
from models.col_schema_models import CollectionSchemaInfoModel, IndexDetailsModel
from utils.doc_schema_ext import DocSchemaExtractor

class CollectionSchemaExtractor:
    
    def __init__(
        self, db_nm: str, col_nm: str, mongo_client: MongoClient, 
        n_doc_to_derive: int = None,
        fetch_doc_batch_size: int = 1000,
        limit_array_elements: int = 10
    ):
        
        self.db_nm = db_nm
        self.col_nm = col_nm
        self.n_doc_to_derive = n_doc_to_derive
        self.fetch_doc_batch_size = fetch_doc_batch_size
        self.collection_schema_info = CollectionSchemaInfoModel()
        
        self.get_schema(mongo_client)
        self.derive_document_schema(mongo_client, n_doc_to_derive=n_doc_to_derive, batch_size=fetch_doc_batch_size, limit_array_elements = limit_array_elements)
        
    def get_schema(self, mongo_client: MongoClient):
        
        try:
            self.collection_schema_info.db_nm = self.db_nm
            self.collection_schema_info.col_nm = self.col_nm
            self.collection_schema_info.fetch_datetime = datetime.now(ZoneInfo("Asia/Hong_Kong"))

            if self.col_nm not in mongo_client[self.db_nm].list_collection_names():
                raise ValueError(f"Collection {self.col_nm} not found in database {self.db_nm}")
            
            
            col_stats = mongo_client[self.db_nm].command("collstats", self.col_nm)
            
            self.collection_schema_info.collection_ns = col_stats.get("ns", None)
            self.collection_schema_info.document_data_size = col_stats.get("size", None)
            self.collection_schema_info.n_documents = col_stats.get("count", None)
            self.collection_schema_info.avg_document_data_size = col_stats.get("avgObjSize", None)
            self.collection_schema_info.document_storage_size = col_stats.get("storageSize", None)
            self.collection_schema_info.index_details = IndexDetailsModel(
                ttl_index_size = col_stats.get("totalIndexSize", None),
                index_sizes = col_stats.get("indexSizes", None),
                settings = mongo_client[self.db_nm][self.col_nm].index_information()
            )
            
        except Exception as e:
            self.collection_schema_info.extract_error = str(e)
        
    
    def derive_document_schema(self, mongo_client: MongoClient, n_doc_to_derive: int = None, batch_size: int = 1000, sort_id_ind: bool = True, limit_array_elements = 10):
        
        try:
            self.collection_schema_info.doc_schema_info.fetch_datetime = datetime.now(ZoneInfo("Asia/Hong_Kong"))
            self.collection_schema_info.doc_schema_info.n_doc_to_derive = n_doc_to_derive
            collect_client = mongo_client[self.db_nm][self.col_nm]
            
            if not isinstance(n_doc_to_derive, int):
                cursor = collect_client.find({})
            else:
                if sort_id_ind: 
                    cursor = collect_client.find({}).sort("_id", DESCENDING).limit(n_doc_to_derive)
                else: 
                    cursor = collect_client.find({}).limit(n_doc_to_derive)
            
            derived_doc_schema_results = None
            while True:
                docs = CollectionSchemaExtractor.fetch_many_by_cursor(cursor, batch_size=batch_size)
                if docs is None: break
            
                derived_doc_schema_results = DocSchemaExtractor.derive_schema(docs, agg_derived_schema=derived_doc_schema_results, limit_array_elements = limit_array_elements)
            
            if derived_doc_schema_results is not None: self.collection_schema_info.doc_schema_info.doc_schema_details = derived_doc_schema_results 
            
        except Exception as e:
            self.collection_schema_info.doc_schema_info.extract_error = str(e)
            
    
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
