import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# Data URL prefix (constant)
DATA_URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

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


@click.command()
@click.option('--db-user', default='root', help='Database user')
@click.option('--db-password', default='root', help='Database password')
@click.option('--db-host', default='localhost', help='Database host')
@click.option('--db-port', default='5432', help='Database port')
@click.option('--db-name', default='ny_taxi', help='Database name')
@click.option('--table-name', default='yellow_taxi_data', help='Table name')
@click.option('--year', default=2021, type=int, help='Year of the data to ingest')
@click.option('--month', default=1, type=int, help='Month of the data to ingest')
@click.option('--chunk-size', default=100000, type=int, help='Chunk size for reading CSV')
def run(db_user, db_password, db_host, db_port, db_name, table_name, year, month, chunk_size):
    """
    Read NYC taxi data from CSV and insert it into PostgreSQL database.
    
    This function:
    1. Downloads and reads the CSV data in chunks
    2. Creates the table schema on first chunk
    3. Appends remaining chunks to the table
    """
    # Build data file name
    data_file = f'yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    # Create database connection
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    # Read CSV data in chunks
    df_iter = pd.read_csv(
        f"{DATA_URL_PREFIX}{data_file}", 
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunk_size
    )
    
    # Process chunks
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table with schema from first chunk
            df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            first = False
        
        # Insert data
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
    
    print(f"Data ingestion completed successfully to table '{table_name}'")


if __name__ == "__main__":
    run()




