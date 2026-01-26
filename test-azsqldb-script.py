# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import os
from config.database_config import DatabaseConfig
database_config = DatabaseConfig("./config/database_config.json")

# COMMAND ----------

database_config.execute_query("SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'TEST_TABLE'")

# COMMAND ----------

sql = """
CREATE TABLE CDLC.TEST_TABLE (
    Id INT PRIMARY KEY,
    Name NVARCHAR(100),
    CreatedDate DATETIME
)
"""

database_config.execute_statement(sql)

# COMMAND ----------

database_config.execute_query("SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'TEST_TABLE'")

# COMMAND ----------

from datetime import datetime

data = [
    (1, 'Alice', datetime(2026, 1, 23, 10, 0, 0)),
    (2, 'Bob', datetime(2026, 1, 23, 11, 0, 0)),
    (3, 'Charlie', datetime(2026, 1, 23, 12, 0, 0))
]

database_config.execute_statement(
    "INSERT INTO CDLC.TEST_TABLE (Id, Name, CreatedDate) VALUES (%d, %s, %s)",
    data
)

# COMMAND ----------

sql = """
SELECT * FROM CDLC.TEST_TABLE
"""
database_config.execute_query(sql)

# COMMAND ----------

sql1 = """
IF NOT EXISTS (
    SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('CDLC.TEST_TABLE') AND is_primary_key = 1
)
BEGIN
    ALTER TABLE CDLC.TEST_TABLE
    ADD CONSTRAINT PK_TEST_TABLE_Id PRIMARY KEY (Id)
END
"""
database_config.execute_statement(sql1)

# COMMAND ----------


sql2 = """
IF NOT EXISTS (
    SELECT * FROM sys.indexes WHERE name = 'IDX_TEST_TABLE_Name'
)
BEGIN
    CREATE INDEX IDX_TEST_TABLE_Name ON CDLC.TEST_TABLE (Name)
END
"""
database_config.execute_statement(sql2)

# COMMAND ----------



# Check primary key
pk_indexes = database_config.execute_query("""
SELECT i.name AS index_name, i.is_primary_key
FROM sys.indexes i
JOIN sys.objects o ON i.object_id = o.object_id
WHERE o.name = 'TEST_TABLE' AND o.schema_id = SCHEMA_ID('CDLC')
""")

# Check all indexes
all_indexes = database_config.execute_query("""
SELECT i.name AS index_name, c.name AS column_name, i.is_primary_key
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.objects o ON i.object_id = o.object_id
WHERE o.name = 'TEST_TABLE' AND o.schema_id = SCHEMA_ID('CDLC')
ORDER BY i.name, ic.key_ordinal
""")

print("Primary Key Indexes:", pk_indexes)
print("All Indexes:", all_indexes)

# COMMAND ----------

# Add a new column 'Email' of type NVARCHAR(255)
database_config.execute_statement("""
ALTER TABLE CDLC.TEST_TABLE
ADD Email NVARCHAR(255)
""")

# Update Email values for each user
update_data = [
    ('alice@example.com', 1),
    ('bob@example.com', 2),
    ('charlie@example.com', 3)
]
database_config.execute_statement(
    "UPDATE CDLC.TEST_TABLE SET Email = %s WHERE Id = %d",
    update_data
)

# COMMAND ----------

sql = """
SELECT * FROM CDLC.TEST_TABLE
"""
database_config.execute_query(sql)

# COMMAND ----------


# Delete Email column
database_config.execute_statement("""
ALTER TABLE CDLC.TEST_TABLE
DROP COLUMN Email
""")


# COMMAND ----------

sql = """
SELECT * FROM CDLC.TEST_TABLE
"""
database_config.execute_query(sql)

# COMMAND ----------

sql = """
TRUNCATE TABLE CDLC.TEST_TABLE
"""

database_config.execute_statement(sql)

# COMMAND ----------

sql = """
SELECT * FROM CDLC.TEST_TABLE
"""
database_config.execute_query(sql)

# COMMAND ----------



# Drop primary key constraint if exists
database_config.execute_statement("""
DECLARE @pk_name NVARCHAR(128)
SELECT @pk_name = kc.name
FROM sys.key_constraints kc
JOIN sys.objects o ON kc.parent_object_id = o.object_id
WHERE kc.type = 'PK' AND o.name = 'TEST_TABLE' AND o.schema_id = SCHEMA_ID('CDLC')
IF @pk_name IS NOT NULL
    EXEC('ALTER TABLE CDLC.TEST_TABLE DROP CONSTRAINT ' + @pk_name)
""")

# Drop index on Name if exists
database_config.execute_statement("""
IF EXISTS (
    SELECT * FROM sys.indexes WHERE name = 'IDX_TEST_TABLE_Name' AND object_id = OBJECT_ID('CDLC.TEST_TABLE')
)
    DROP INDEX IDX_TEST_TABLE_Name ON CDLC.TEST_TABLE
""")


# COMMAND ----------



# Check primary key
pk_indexes = database_config.execute_query("""
SELECT i.name AS index_name, i.is_primary_key
FROM sys.indexes i
JOIN sys.objects o ON i.object_id = o.object_id
WHERE o.name = 'TEST_TABLE' AND o.schema_id = SCHEMA_ID('CDLC')
""")

# Check all indexes
all_indexes = database_config.execute_query("""
SELECT i.name AS index_name, c.name AS column_name, i.is_primary_key
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.objects o ON i.object_id = o.object_id
WHERE o.name = 'TEST_TABLE' AND o.schema_id = SCHEMA_ID('CDLC')
ORDER BY i.name, ic.key_ordinal
""")

print("Primary Key Indexes:", pk_indexes)
print("All Indexes:", all_indexes)

# COMMAND ----------

database_config.execute_statement("""
EXEC sp_rename 'CDLC.TEST_TABLE', 'TEST_TABLE1'
""")

# COMMAND ----------

database_config.execute_query("SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'TEST_TABLE'")

# COMMAND ----------

sql = """
DROP TABLE CDLC.TEST_TABLE1
"""
database_config.execute_statement(sql)

# COMMAND ----------

database_config.execute_query("SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'TEST_TABLE'")
