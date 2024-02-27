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
    # athena_client = boto3.client('athena')
    # database = 'commdatabase'
    
    s3_client = boto3.client('s3')
    s3_bucket = 's3bucket-comm'
    
    # # Commodities dict
    # commodities_dict = {"Cotton": ["COTTON NO. 2 - ICE FUTURES U.S.", "COTTON NO. 2 - NEW YORK BOARD OF TRADE"],
    #                     "WheatSRW": ["WHEAT-SRW - CHICAGO BOARD OF TRADE", "WHEAT - CHICAGO BOARD OF TRADE"],
    #                     "WheatHRW": ["WHEAT-HRW - CHICAGO BOARD OF TRADE", "WHEAT - KANSAS CITY BOARD OF TRADE"],
    #                     "WheatHRSpring": ["WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE", "WHEAT - MINNEAPOLIS GRAIN EXCHANGE"],
    #                     "Corn": ["CORN - CHICAGO BOARD OF TRADE"],
    #                     "Soybeans": ["SOYBEANS - CHICAGO BOARD OF TRADE"],
    #                     "SoybeanOil": ["SOYBEAN OIL - CHICAGO BOARD OF TRADE"],
    #                     "SoybeanMeal": ["SOYBEAN MEAL - CHICAGO BOARD OF TRADE"],
    #                     "RoughRice": ["ROUGH RICE - CHICAGO BOARD OF TRADE"],
    #                     "FrznOJ": ["FRZN CONCENTRATED ORANGE JUICE - ICE FUTURES U.S."],
    #                     "Butter": ["BUTTER (CASH SETTLED) - CHICAGO MERCANTILE EXCHANGE"],
    #                     "MilkClassIII": ["MILK, Class III - CHICAGO MERCANTILE EXCHANGE"],
    #                     "NonFatDryMilk": ["NON FAT DRY MILK - CHICAGO MERCANTILE EXCHANGE"],
    #                     "CMEMilkIV": ["CME MILK IV - CHICAGO MERCANTILE EXCHANGE"],
    #                     "Cheese": ["CHEESE (CASH-SETTLED) - CHICAGO MERCANTILE EXCHANGE"],
    #                     "LeanHogs": ["LEAN HOGS - CHICAGO MERCANTILE EXCHANGE"],
    #                     "LiveCattle": ["LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE"],
    #                     "FeederCattle": ["FEEDER CATTLE - CHICAGO MERCANTILE EXCHANGE"],
    #                     "Cocoa": ["COCOA - ICE FUTURES U.S.", "COCOA - NEW YORK BOARD OF TRADE"],
    #                     "Sugar": ["SUGAR NO. 11 - ICE FUTURES U.S.", "SUGAR NO. 11 - NEW YORK BOARD OF TRADE"],
    #                     "Coffee": ["COFFEE C - ICE FUTURES U.S.", "COFFEE C - NEW YORK BOARD OF TRADE"],
    #                     "PalmOil": ["USD Malaysian Crude Palm Oil C - CHICAGO MERCANTILE EXCHANGE"],
    #                     "Canola": ["CANOLA - ICE FUTURES U.S."]}
    
    # # Dict of pandas dataframes, one for each of the keys in the above dict
    # dataframe_dict = {}
    # for key in commodities_dict.keys():
    #     dataframe_dict.append()
    
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