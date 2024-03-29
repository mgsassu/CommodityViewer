import pandas as pd
import numpy as np
import datetime as dt
import glob
import os

import boto3
from io import BytesIO

def main():
    # Current time
    t1 = dt.datetime.now()
    t2 = str(t1.replace(microsecond=0))
    t3 = t2.replace(":","-")
    t4 = t3.replace(" ","_")
    dt_now_str = str(t4)
    
    # Setup AWS clients and locations
    s3_client = boto3.client('s3')
    s3_bucket = 's3bucket-comm'
    
    # Get all files
    path = "./OutputFiles_Reduced/*.csv"
    files = glob.glob(path)
    
    # Loop through files, append to the appropriate pandas dataframe
    for file in files:
        # Get the filename, will be used to make a new S3 folder
        filex = os.path.basename(file)
        key = filex.split('.')[0]
        
        # read in the file
        print(f"Reading in {file}...")
        df = pd.read_csv(file, index_col=False)
        
        ### Now upload the dataset ######################
        # Create a path for the data. NOTE: The text "commodity=key" is required for the Hive format in S3, which allows the data to be queried by Athena. 
        path = f'commodities/commodity={key}/data.parquet'

        # Convert the data to Parquet (in-memory)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)

        # Upload the Parquet data to S3
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=path,
            Body=parquet_buffer.getvalue()
        )

        # Print confirmation message
        print(f"File '{key}' loaded successfully.")


if __name__ == "__main__":
    main()