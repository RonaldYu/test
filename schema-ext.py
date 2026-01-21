# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC %pip install pandas
# MAGIC %pip install pydantic --upgrade
# MAGIC %pip install pymongo

# COMMAND ----------

import sys

print(f"Python version: {sys.version}")

# COMMAND ----------

from pymongo import MongoClient
import os
import pandas as pd

from utils.db_schema_ext import DatabaseSchemaExtractor
from utils.reporting_opr import DBAnalysisReportingUtils
from models.reporting_models import DatabaseSchemaReportingModel
from contextlib import contextmanager


# COMMAND ----------

# dbutils.widgets.removeAll()
# cluster_nm = 'db-pruforce-connection-uat'
cluster_nm = 'db-esub2-connection'
scope_nm = "scope-cloudedw"
# scope_nm, cluster_nm = "", ""
dbutils.widgets.text("mongo-cnnt-str-secret-scope-nm", scope_nm)
dbutils.widgets.text("mongo-cnnt-str-secret-key-nm", cluster_nm)

report_nm = dbutils.widgets.get('mongo-cnnt-str-secret-key-nm')

@contextmanager
def mongo_client_ctx():
  client = MongoClient(
    dbutils.secrets.get(
      scope = dbutils.widgets.get('mongo-cnnt-str-secret-scope-nm'),
      key = dbutils.widgets.get('mongo-cnnt-str-secret-key-nm')
    )
  )
  try:
    yield client
  finally:
    try: client.close()
    except: pass

# COMMAND ----------

from collections import defaultdict
db_schema_results = defaultdict(dict)
from concurrent.futures import ThreadPoolExecutor

def process_db(dbschema_ext, db_nm, col_nm, col_idx, ttl_cols):
  try:
    with mongo_client_ctx() as mongo_client:
      ls_col_nm = [col_nm] if col_nm is not None else None
      try: n_docs = mongo_client[db_nm][col_nm].count_documents({})
      except: n_docs = -1
      print(f'##### Execute {db_nm}.{col_nm} ({n_docs}): {col_idx}/{ttl_cols}\n')

      try: 
        n_doc_to_derive = min([n_docs, 100000])
        if n_doc_to_derive <= 0: n_doc_to_derive = None
      except: n_doc_to_derive = None

      dbschema_ext.get_collection_schema(
        mongo_client = mongo_client, 
        ls_col_nm=ls_col_nm, n_doc_to_derive=n_doc_to_derive, 
        fetch_doc_batch_size=1000,
        limit_array_elements = 3
      )
  except Exception as e:
    
    # import traceback
    # print(f'##### Error {db_nm}.{col_nm}: {str(e)}')
    print(f'##### Error {db_nm}.{col_nm}: {col_idx}/{ttl_cols}\n')
    # traceback.print_exc()
   

with mongo_client_ctx() as mongo_client:
  db_names = mongo_client.list_database_names()
  db_names = db_names

ls_db_cols_pair = []
with mongo_client_ctx() as mongo_client:
  for db_nm in db_names:
    try:
      for col_nm in mongo_client[db_nm].list_collection_names():
        ls_db_cols_pair.append((db_nm, col_nm))
    except Exception as e:
      ls_db_cols_pair.append((db_nm, None))

with mongo_client_ctx() as mongo_client:
  for db_idx, db_nm in enumerate(db_names):
    print(f"Extract general database level schema: {db_idx} / {len(db_names)}")
    db_schema_results[db_nm] = DatabaseSchemaExtractor(db_nm=db_nm, mongo_client=mongo_client)

with ThreadPoolExecutor(max_workers=5) as executor:
  futures = []
  for col_idx, (db_nm, col_nm) in enumerate(ls_db_cols_pair):
    futures.append(
      executor.submit(process_db, db_schema_results[db_nm], db_nm, col_nm, col_idx+1, len(ls_db_cols_pair))
    )
  for future in futures:
    try:
      _ = future.result()
    except Exception as e:
      # import traceback
      # print(f'##### Error in future for {db_nm}.{col_nm}: {str(e)}')
      pass
  


# COMMAND ----------

# from collections import defaultdict
# db_schema_results = defaultdict(dict)
# from concurrent.futures import ThreadPoolExecutor

# def process_db(dbschema_ext, db_nm, col_nm, col_idx, ttl_cols):
#   try:
#     with mongo_client_ctx() as mongo_client:
#       ls_col_nm = [col_nm] if col_nm is not None else None
#       try: n_docs = mongo_client[db_nm][col_nm].count_documents({})
#       except: n_docs = -1
#       print(f'##### Execute {db_nm}.{col_nm} ({n_docs}): {col_idx}/{ttl_cols}\n')

#       try: 
#         n_doc_to_derive = min([n_docs, 100000])
#         if n_doc_to_derive <= 0: n_doc_to_derive = None
#       except: n_doc_to_derive = None

#       dbschema_ext.get_collection_schema(
#         mongo_client = mongo_client, 
#         ls_col_nm=ls_col_nm, n_doc_to_derive=n_doc_to_derive, 
#         fetch_doc_batch_size=1000,
#         limit_array_elements = 3
#       )
#   except Exception as e:
    
#     # import traceback
#     # print(f'##### Error {db_nm}.{col_nm}: {str(e)}')
#     print(f'##### Error {db_nm}.{col_nm}: {col_idx}/{ttl_cols}\n')
#     # traceback.print_exc()
   

# with mongo_client_ctx() as mongo_client:
#   db_names = mongo_client.list_database_names()
#   db_names = db_names

# ls_db_cols_pair = []
# with mongo_client_ctx() as mongo_client:
#   for db_nm in db_names:
#     try:
#       for col_nm in mongo_client[db_nm].list_collection_names():
#         ls_db_cols_pair.append((db_nm, col_nm))
#     except Exception as e:
#       ls_db_cols_pair.append((db_nm, None))

# with mongo_client_ctx() as mongo_client:
#   for db_idx, db_nm in enumerate(db_names):
#     print(f"Extract general database level schema: {db_idx} / {len(db_names)}")
#     db_schema_results[db_nm] = DatabaseSchemaExtractor(db_nm=db_nm, mongo_client=mongo_client)

# for col_idx, (db_nm, col_nm) in enumerate(ls_db_cols_pair):
#   process_db(
#     dbschema_ext = db_schema_results[db_nm], 
#     db_nm = db_nm, col_nm = col_nm, col_idx = col_idx+1, ttl_cols = len(ls_db_cols_pair)
#   )
  


# COMMAND ----------

result_output_dir = 'results'
result_json_output_dir = os.path.join(result_output_dir, 'json')

result_tabular_output_dir = os.path.join(
  result_output_dir, 'table'
)

# COMMAND ----------

os.makedirs(result_json_output_dir, exist_ok=True)
os.makedirs(result_tabular_output_dir, exist_ok=True)

# COMMAND ----------

with mongo_client_ctx() as mongo_client:
  db_schema_report = DatabaseSchemaReportingModel(
    host = mongo_client.address[0],
    list_database_schema_info = [
      res.database_schema_info for db_nm, res in db_schema_results.items()
    ]
  )

# COMMAND ----------

DBAnalysisReportingUtils.dbanalysis_report_to_json(
  obj = db_schema_report,
  file_path = os.path.join(result_json_output_dir, report_nm + '.json')
)

# COMMAND ----------

DBAnalysisReportingUtils.dbanalysis_report_to_excel(
  obj = db_schema_report,
  file_path = os.path.join(result_tabular_output_dir, report_nm + '.xlsx')
)
