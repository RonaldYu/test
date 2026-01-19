from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo

class DatabaseSchemaModel:

    def __init__(self, db_nm: str, mongo_client: MongoClient):

        self.db_nm = db_nm

        self.get_schema(mongo_client)
    
    def get_schema(self, mongo_client: MongoClient):

        db_stats = mongo_client[self.db_nm].command("dbstats")

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