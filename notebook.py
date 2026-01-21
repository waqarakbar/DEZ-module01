#!/usr/bin/env python
# coding: utf-8

# In[45]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# In[53]:


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# df = pd.read_csv(f"{prefix}yellow_tripdata_2021-01.csv.gz", dtype=dtype, parse_dates=parse_dates)
df_iter = pd.read_csv(
    f"{prefix}yellow_tripdata_2021-01.csv.gz", 
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[62]:


# df.head(1)
df_iter


# In[38]:


#df.dtypes
#df.shape
#df['VendorID']


# In[40]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[41]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[43]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[61]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name="yellow_taxi_data", con=engine, if_exists='append')


# In[ ]:




