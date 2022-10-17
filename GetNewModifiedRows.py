# https://blog.devgenius.io/python-etl-pipeline-the-incremental-data-load-techniques-20bdedaae8f

import pandas as pd


# Source data
source = pd.read_sql_query(""" SELECT top 10 * FROM dbo.DimCustomer; """, src_conn)

# Target data 
target = pd.read_sql('Select * from public."stg_IncrementalLoadTest"', engine)

# 1. detect changes. Get rows that are not present in the target.
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

# 2. Get new records
inserts = changes[~changes.CustomerKey.isin(target.CustomerKey)]

# 3. Get modified rows
modified = changes[changes.CustomerKey.isin(target.CustomerKey)]