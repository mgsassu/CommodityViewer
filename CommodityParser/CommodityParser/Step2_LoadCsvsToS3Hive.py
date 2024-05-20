import pandas as pd
import numpy as np
import datetime as dt
import glob
import os
import json

import boto3
from io import BytesIO

import plotly.express as px
import plotly.utils as pu

def main():
    # Current time
    t1 = dt.datetime.now()
    t2 = str(t1.replace(microsecond=0))
    t3 = t2.replace(":","-")
    t4 = t3.replace(" ","_")
    dt_now_str = str(t4)
    
    # Setup AWS clients and locations
    s3_client = boto3.client('s3')
    s3_bucket = 's3bucket-comm-json'
    
    # Get all files
    path = "./OutputFiles_Reduced/*.xlsx"
    files = glob.glob(path)
    
    # Loop through files, append to the appropriate pandas dataframe
    for file in files:
        # Get the filename, will be used to make a new S3 folder
        filex = os.path.basename(file)
        commodity = filex.split('.')[0]
        
        # read in the file
        print(f"Reading in {file}...")
        df = pd.read_excel(file, index_col=False)
        
        # Create the figure
        fig = px.line(df, x = 'Date', y = df.columns[1:], hover_data={"Date": "|%B %d, %Y"}, title = f"{commodity}: Most recent data from {str(df['Date'].iloc[-1].date())}", markers=True)
        fig.update_layout(font=dict(size=20))
        fig.update_layout(hoverlabel=dict(font_size=16))
        
        ### Now upload the dataset ######################
        # Create a path for the data. NOTE: The text "commodity=key" is required for the Hive format in S3, which allows the data to be queried by Athena. 
        path = f'commodities/{commodity}.json'

        # Upload the json data to S3
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=path,
            Body=json.dumps(fig, cls=pu.PlotlyJSONEncoder)
        )

        # Print confirmation message
        print(f"File '{commodity}' loaded successfully.")


if __name__ == "__main__":
    main()