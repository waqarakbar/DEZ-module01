import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# Database connection parameters
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ny_taxi'
TABLE_NAME = 'yellow_taxi_data'

# Data source configuration
DATA_URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
YEAR = 2021
MONTH = 1
DATA_FILE = f'yellow_tripdata_{YEAR}-{MONTH:02d}.csv.gz'
CHUNK_SIZE = 100000

# Data type definitions
DTYPE = {
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

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    """
    Read NYC taxi data from CSV and insert it into PostgreSQL database.
    
    This function:
    1. Downloads and reads the CSV data in chunks
    2. Creates the table schema on first chunk
    3. Appends remaining chunks to the table
    """
    # Create database connection
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_string)
    
    # Read CSV data in chunks
    df_iter = pd.read_csv(
        f"{DATA_URL_PREFIX}{DATA_FILE}", 
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=CHUNK_SIZE
    )
    
    # Process chunks
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table with schema from first chunk
            df_chunk.head(n=0).to_sql(name=TABLE_NAME, con=engine, if_exists='replace')
            first = False
        
        # Insert data
        df_chunk.to_sql(name=TABLE_NAME, con=engine, if_exists='append')
    
    print(f"Data ingestion completed successfully to table '{TABLE_NAME}'")


if __name__ == "__main__":
    run()




