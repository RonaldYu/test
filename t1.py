# Databricks notebook source
connection_str = dbutils.secrets.get(scope="scope-cloudedw",key="db-esub2-connection")

# COMMAND ----------

# df = spark.read.format("mongo") \
#   .option("uri", connection_str) \
#   .option("readPreference", "secondaryPreferred") \
#   .option("inferSchema.mapTypes.enabled", "true") \
#   .option("database", "esub20") \
#   .option('sampleSize', 1000)\
#   .option("collection", "auw_case_info") \
#   .load().limit(10)
 
# display(df)

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

mongo_client = MongoClient(connection_str)

# COMMAND ----------

cursor = mongo_client['esub20']['auw_case_info'].aggregate(
  [
    {
      "$sort": { "_id": 1 }
    },
    {
      "$limit": 3
    },
    {
      "$project": {
        "_id": 1,
        "json": { 
          "$function": {
            "body": "function(doc) { return JSON.stringify(doc) }", 
            "args": ["$$ROOT"], 
            "lang": "js"
          }
        } 
      }
    }
  ]
)

data = list(cursor)

# COMMAND ----------

# DBTITLE 1,Untitled
pipeline_str = """[
  {
    "$sort": { "_id": 1 }
  },
  {
    "$limit": 3
  },
  {
    "$project": {
      "_id": 1,
      "json": { 
        "$function": {
          "body": "function(doc) { return JSON.stringify(doc) }", 
          "args": ["$$ROOT"], 
          "lang": "js"
        }
      } 
    }
  }
]"""

df = spark.read.format("mongo") \
    .option("uri", connection_str) \
    .option("database", "esub20") \
    .option("collection", "auw_case_info") \
    .option("pipeline", pipeline_str) \
    .option("schema", "struct<_id:struct<oid:string>,json:string>") \
    .load()


display(df)

# COMMAND ----------



# COMMAND ----------

cursor = mongo_client['esub20']['auw_case_info'].find({})

# COMMAND ----------

from pymongo.cursor import Cursor
from typing import List, Dict

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

# COMMAND ----------

data = fetch_many_by_cursor(cursor, 100)

# COMMAND ----------

from models.doc_schema_models import DataSchemaInfoModel

# COMMAND ----------

from typing import List, Any, Mapping
from collections import defaultdict

# COMMAND ----------


class DocSchemaExtractor:

    @staticmethod
    def chk_list_data_type(obj: Any):
        return isinstance(obj, list)

    @staticmethod
    def chk_mapping_data_type(obj: Any):
        return isinstance(obj, Mapping)

    @staticmethod
    def get_data_type(obj: Any):

        if obj is None:
            return DataSchemaInfoModel.null_data_type
        elif DocSchemaExtractor.chk_list_data_type(obj):
            return DataSchemaInfoModel.list_data_type
        elif DocSchemaExtractor.chk_mapping_data_type(obj):
            return DataSchemaInfoModel.mapping_data_type
        else:
            try:
                _name = type(obj).__name__
                _name = _name.strip().lower()
            except:
                _name = str(None).strip().lower()
            
            return _name


    @staticmethod
    def ext_schema_tree(
        obj: Any,
        data_nm: str = DataSchemaInfoModel.root_data_nm,
        parent_data_nm: str = None,
        parent_data_type: str = None,
        limit_array_elements: int = -1
    ) -> DataSchemaInfoModel:

        if DocSchemaExtractor.chk_mapping_data_type(obj):
            data_schema_info = DataSchemaInfoModel(
                data_nm=data_nm,
                data_type=DocSchemaExtractor.get_data_type(obj),
                parent_data_nm=parent_data_nm,
                parent_data_type=parent_data_type,
            )

            for k, v in obj.items():
                child = DocSchemaExtractor.ext_schema_tree(
                    obj = v,
                    data_nm = k,
                    parent_data_nm = data_schema_info.data_nm,
                    parent_data_type = data_schema_info.data_type,
                    limit_array_elements = limit_array_elements
                )
                data_schema_info.add_child(child)

        elif DocSchemaExtractor.chk_list_data_type(obj):
            data_schema_info = DataSchemaInfoModel(
                data_nm=data_nm, data_type=DocSchemaExtractor.get_data_type(obj)
            )
            if limit_array_elements < 0: n_array_from = 0
            elif len(obj) > limit_array_elements: n_array_from = len(obj) - limit_array_elements
            else: n_array_from = 0
            n_array_from = int(max(0, n_array_from))
            for item in obj[n_array_from:]:
                child = DocSchemaExtractor.ext_schema_tree(
                    obj=item,
                    data_nm=data_nm + DataSchemaInfoModel.array_item_data_nm,
                    parent_data_nm=parent_data_nm,
                    parent_data_type=parent_data_type,
                    limit_array_elements = limit_array_elements
                )

                data_schema_info.add_child(child)

        else:
            data_schema_info = DataSchemaInfoModel(
                data_nm=data_nm,
                data_type=DocSchemaExtractor.get_data_type(obj = obj),
                parent_data_nm=parent_data_nm,
                parent_data_type=parent_data_type,
            )

        return data_schema_info


derived_schema = dict()

# COMMAND ----------

tmp = DocSchemaExtractor.ext_schema_tree(data[0])

# COMMAND ----------

tmp.model_dump()

# COMMAND ----------

data = [{'x': 1, 'y': {'z': 1, 'y': 2}}, {'x': 1, 'y': {'z': 1, 'x': 'a'}}]

# COMMAND ----------

