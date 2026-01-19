from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict
from col_schma_ext import CollectionSchemaModel

class DatabaseSchemaModel:

    def __init__(self, db_nm: str, mongo_client: MongoClient):

        if db_nm not in mongo_client.list_database_names():
            raise ValueError(f"Database {db_nm} not found")
        
        self.db_nm = db_nm

        self.get_schema(mongo_client)
        self.collection_schema_details: Dict[str, CollectionSchemaModel] = dict()
    
    def get_schema(self, mongo_client: MongoClient):

        db_stats = mongo_client[self.db_nm].command("dbstats")

        self.host, self.port = mongo_client.address
        
        self.n_collections = db_stats.get('collections', None)
        self.n_views = db_stats.get('views', None)
        self.n_documents = db_stats.get('objects', None)
        self.avg_document_data_size = db_stats.get('avgObjSize', None)
        self.document_data_size = db_stats.get('dataSize', None)
        self.document_storage_size = db_stats.get('storageSize', None)
        self.fetch_datetime = datetime.now(ZoneInfo("Asia/Hong_Kong"))
        self.index_details = {
            'n_indexes': db_stats.get('indexes', None),
            'ttl_index_size': db_stats.get('indexSize', None)
        }

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
            'db_nm': self.db_nm,
            'host': self.host,
            'port': self.port,
            'n_collections': self.n_collections,
            'n_views': self.n_views,
            'n_documents': self.n_documents,
            'avg_document_data_size': self.avg_document_data_size,
            'document_data_size': self.document_data_size,
            'document_storage_size': self.document_storage_size,
            'index_details': self.index_details,
            'fetch_datetime': self.fetch_datetime,
            'collection_schema_details': self.collection_schema_details
        }