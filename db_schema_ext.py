from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List
import copy
from utils.col_schma_ext import CollectionSchemaModel
from utils.general_opr import _dict_to_json_serializable

class DatabaseLevelSchemaInfoModel:
    def __init__(
        self,
        host: str, 
        port: int,
        db_nm: str,
        n_views: int,
        n_collections: int,
        n_documents: int,
        avg_document_data_size: float,
        document_data_size: int,
        document_storage_size: int,
        ttl_n_indexes: int,
        ttl_index_size: int,
        fetch_datetime: datetime,
    ):

        self.host = host
        self.port = port
        self.db_nm = db_nm
        self.n_views = n_views
        self.n_collections = n_collections
        self.n_documents = n_documents
        self.avg_document_data_size = avg_document_data_size
        self.document_data_size = document_data_size
        self.document_storage_size = document_storage_size
        self.ttl_n_indexes = ttl_n_indexes
        self.ttl_index_size = ttl_index_size
        self.fetch_datetime = fetch_datetime

    def to_dict(self):
        return {
            'host': self.host,
            'port': self.port,
            'db_nm': self.db_nm,
            'n_views': self.n_views,
            'n_documents': self.n_documents,
            'avg_document_data_size': self.avg_document_data_size,
            'document_data_size': self.document_data_size,
            'document_storage_size': self.document_storage_size,
            'ttl_n_indexes': self.ttl_n_indexes,
            'ttl_index_size': self.ttl_index_size,
            'fetch_datetime': self.fetch_datetime
        }

class DatabaseSchemaModel:

    def __init__(self, db_nm: str, mongo_client: MongoClient):

        if db_nm not in mongo_client.list_database_names():
            raise ValueError(f"Database {db_nm} not found")
        
        self.db_nm = db_nm

        self.get_schema(mongo_client)
        self.collection_schema_details: Dict[str, CollectionSchemaModel] = dict()
    
    def get_schema(self, mongo_client: MongoClient):

        db_stats = mongo_client[self.db_nm].command("dbstats")

        self.database_schema_info = DatabaseLevelSchemaInfoModel(
            host = mongo_client.address[0],
            port = mongo_client.address[1],
            db_nm = self.db_nm,
            n_collections = db_stats.get('collections', None),
            n_views = db_stats.get('views', None),
            n_documents = db_stats.get('objects', None),
            avg_document_data_size = db_stats.get('avgObjSize', None),
            document_data_size = db_stats.get('dataSize', None),
            document_storage_size = db_stats.get('storageSize', None),
            ttl_n_indexes = db_stats.get('indexes', None),
            ttl_index_size = db_stats.get('indexSize', None),
            fetch_datetime = datetime.now(ZoneInfo("Asia/Hong_Kong"))
        )
        
        

    def get_collection_schema(self, col_nm: str, mongo_client: MongoClient, n_doc_to_derive: int = None, fetch_doc_batch_size: int = 1000):

        if col_nm not in mongo_client[self.db_nm].list_collection_names():
            raise ValueError(f"Collection {col_nm} not found in database {self.db_nm}")
        
        self.collection_schema_details[col_nm] = CollectionSchemaModel(
            db_nm = self.db_nm, col_nm = col_nm, mongo_client = mongo_client,
            n_doc_to_derive = n_doc_to_derive, fetch_doc_batch_size = fetch_doc_batch_size
        )

    def get_all_collections_schema(self, mongo_client: MongoClient, n_doc_to_derive: int = None, fetch_doc_batch_size: int = 1000):

        for col_nm in mongo_client[self.db_nm].list_collection_names():
            self.get_collection_schema(col_nm, mongo_client, n_doc_to_derive, fetch_doc_batch_size)

    def to_dict(self):
        return {
            'database_schema_info': self.database_schema_info,
            'collection_schema_details': self.collection_schema_details
        }

class DBSchemaAnalysisUtils:

    @staticmethod
    def database_schema_info_to_tabular_format(obj: DatabaseSchemaModel):
        return _dict_to_json_serializable(obj.database_schema_info)

    @staticmethod
    def collection_schema_info_to_tabular_format(obj: DatabaseSchemaModel):
        
        collection_schema_info_tabular_format = []

        for col_nm, collection_detail in obj.collection_schema_details.items():
            tmp = {
                k: v
                for k, v in collection_detail.collection_schema_info.to_dict().items()
                if k != 'index_details'
            }
            tmp['ttl_index_size'] = collection_detail.collection_schema_info.index_details.get('ttl_index_size', None)
            collection_schema_info_tabular_format.append(tmp)
    
        return _dict_to_json_serializable(collection_schema_info_tabular_format)
    
    @staticmethod
    def collection_index_details_to_tabular_format(obj: DatabaseSchemaModel):
        collection_index_details_tabular_format = []

        for col_nm, collection_detail in obj.collection_schema_details.items():

            ttl_size = collection_detail.collection_schema_info.index_details.get('index_sizes', dict())
            settings = collection_detail.collection_schema_info.index_details.get('settings', dict({}))
            tmp_settings = copy.deepcopy(settings)
            for index_name in tmp_settings.keys():
                
                try:
                    tmp_settings[index_name]['index_size'] += ttl_size.get(index_name, 0)
                except:
                    tmp_settings[index_name]['index_size'] = ttl_size.get(index_name, 0)

                
                tmp_settings[index_name]['db_nm'] = collection_detail.collection_schema_info.db_nm
                tmp_settings[index_name]['col_nm'] = collection_detail.collection_schema_info.col_nm
                tmp_settings[index_name]['collection_ns'] = collection_detail.collection_schema_info.collection_ns
                tmp_settings[index_name]['index_name'] = index_name
                tmp_settings[index_name]['fetch_datetime'] = collection_detail.collection_schema_info.fetch_datetime

            collection_index_details_tabular_format.append(tmp_settings)

        return _dict_to_json_serializable(collection_index_details_tabular_format)

    @staticmethod
    def doc_schema_details_to_tabular_format(obj: DatabaseSchemaModel):
        doc_schema_details_tabular_format = []

        for col_nm, collection_detail in obj.collection_schema_details.items():

            for data_nm, data_schema in collection_detail.doc_schema_details.items():
                doc_schema_details_tabular_format.append({
                    'db_nm': collection_detail.collection_schema_info.db_nm,
                    'col_nm': collection_detail.collection_schema_info.col_nm,
                    'collection_ns': collection_detail.collection_schema_info.collection_ns,
                    'fetch_datetime': collection_detail.collection_schema_info.fetch_datetime,
                    **data_schema.to_dict()
                })
        return _dict_to_json_serializable(doc_schema_details_tabular_format)