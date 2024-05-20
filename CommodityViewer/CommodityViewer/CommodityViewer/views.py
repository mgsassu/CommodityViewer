"""
Routes and views for the flask application.
"""

from datetime import datetime
from flask import Flask, render_template, request, jsonify
from CommodityViewer import application

import os
import pandas as pd
import numpy as np
import datetime as dt
import time
import glob
import json
import traceback

import openpyxl
import plotly
import plotly.express as px

import boto3

# @application.route('/home')
# def home():
#     """Renders the home page."""
#     return render_template(
#         'index.html',
#         title='Home Page',
#         year=datetime.now().year,
#     )

# @application.route('/contact')
# def contact():
#     """Renders the contact page."""
#     return render_template(
#         'contact.html',
#         title='Contact',
#         year=datetime.now().year,
#         message='Your contact page.'
#     )

# @application.route('/about')
# def about():
#     """Renders the about page."""
#     return render_template(
#         'about.html',
#         title='About',
#         year=datetime.now().year,
#         message='Your application description page.'
#     )

@application.route('/')
@application.route('/commodities')
def commodities():
    try:
        # # Get all commodities, and display them
        # # Set up Athena client
        # athena_client = boto3.client('athena', region_name=REGION)

        # # Query Athena
        # query = f"SHOW PARTITIONS commodities"
        # response = athena_client.start_query_execution(
        #     QueryString=query,
        #     QueryExecutionContext={
        #         'Database': DATABASE
        #     },
        #     ResultConfiguration={
        #         'OutputLocation': S3_OUTPUT,
        #     }
        # )

        # query_execution_id = response['QueryExecutionId']

        # max_retries = 3  # Maximum number of retries
        # backoff_factor = 2  # Factor by which the wait time will be multiplied
        # wait_time = 0.5  # Initial wait time in seconds

        # retries = 0
        # while retries < max_retries:
        #     # Check query status
        #     response = athena_client.get_query_execution(
        #         QueryExecutionId=query_execution_id
        #     )
        #     status = response['QueryExecution']['Status']['State']
        #     if status in ['RUNNING', 'QUEUED']:
        #         print(f'Query is {status}. Retrying in {wait_time} seconds...')
        #         time.sleep(wait_time)
        #         # Exponential backoff with a maximum of 8 seconds (total wait time of ~55 seconds)
        #         # if we wait any longer, we risk hitting the 15 minute Lambda timeout
        #         if wait_time < 8:
        #             wait_time *= backoff_factor
        #         retries += 1
        #     elif status == 'SUCCEEDED':
        #         print('Query is complete! Creating commodity list...')
        #         break
        #     else:
        #         message = f'Failed to load partition list from Athena with status: {status}'
        #         print(message)
        #         return jsonify(error=str(message)), 500
    
        # # Need to call this to get the actual response data
        # response = athena_client.get_query_results(
        #     QueryExecutionId=query_execution_id
        # )

        # Setup AWS clients and locations
        s3_client = boto3.client('s3')
        s3_bucket_name = 's3bucket-comm-json'

    
        commodities = []
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix='commodities/')
        for key in response['Contents']:
            # Note: We have some fun string parsing below to get just the name of the commodity out of the file path in the S3 bucket.
            commodity_name = key['Key'].split('/')[1].split('.')[0]
            if commodity_name == "Cotton":
                continue
            commodities.append(commodity_name)
            
        # Sort, and put Cotton at the top
        commodities.sort()
        commodities.insert(0, 'Cotton')
    
        """Renders the commodities page."""
        return render_template(
            'commodities.html',
            title='Commodities',
            year=datetime.now().year,
            message='The commodities graph page.',
            keys=commodities  # Serialize data to JSON
        )
    except Exception as e:
        return jsonify(error=str(e), stack_trace=traceback.format_exc()), 500

@application.route('/get_graph', methods=['POST'])
def get_graph():
    try:    
        # Get commodity
        commodity = request.form['commodity']
    
        # # Set up Athena client
        # athena_client = boto3.client('athena', region_name=REGION)

        # # Query Athena
        # query = f"SELECT * FROM commodities WHERE commodity='{commodity}'"
        # response = athena_client.start_query_execution(
        #     QueryString=query,
        #     QueryExecutionContext={
        #         'Database': DATABASE
        #     },
        #     ResultConfiguration={
        #         'OutputLocation': S3_OUTPUT,
        #     }
        # )

        # query_execution_id = response['QueryExecutionId']

        # max_retries = 3  # Maximum number of retries
        # backoff_factor = 2  # Factor by which the wait time will be multiplied
        # wait_time = 0.5  # Initial wait time in seconds

        # retries = 0
        # while retries < max_retries:
        #     # Check query status
        #     response = athena_client.get_query_execution(
        #         QueryExecutionId=query_execution_id
        #     )
        #     status = response['QueryExecution']['Status']['State']
        #     if status in ['RUNNING', 'QUEUED']:
        #         print(f'Query is {status}. Retrying in {wait_time} seconds...')
        #         time.sleep(wait_time)
        #         # Exponential backoff with a maximum of 8 seconds (total wait time of ~55 seconds)
        #         # if we wait any longer, we risk hitting the 15 minute Lambda timeout
        #         if wait_time < 8:
        #             wait_time *= backoff_factor
        #         retries += 1
        #     elif status == 'SUCCEEDED':
        #         print('Query is complete! Creating dataframe...')
        #         break
        #     else:
        #         message = f'Failed to load partition list from Athena with status: {status}'
        #         print(message)
        #         return jsonify(error=str(message)), 500
    
        # # Need to call this to get the actual response data
        # response = athena_client.get_query_results(
        #     QueryExecutionId=query_execution_id
        # )
    
        # # Create dataframe from response
        # column_names = [col['Name'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        # rows = response['ResultSet']['Rows'][1:]  # Skip column names row
        # data = [[field['VarCharValue'] for field in row['Data']] for row in rows]
        # df = pd.DataFrame(data, columns=column_names)
        # df = df.drop(columns=['commodity'])
        # df['date'] = pd.to_datetime(df['date'])
        # df['prod_net'] = pd.to_numeric(df['prod_net'])
        # df['swap_net'] = pd.to_numeric(df['swap_net'])
        # df['mm_net'] = pd.to_numeric(df['mm_net'])
        # df['other_reportable_net'] = pd.to_numeric(df['other_reportable_net'])
        # df['non_reportable_net'] = pd.to_numeric(df['non_reportable_net'])
        # df = df.sort_values(by='date')

        # # Create the figure
        # fig = px.line(df, x = 'date', y = df.columns[1:], hover_data={"date": "|%B %d, %Y"}, title = f"{commodity}: Most recent data from {str(df['date'].iloc[-1].date())}", markers=True)
        # fig.update_layout(font=dict(size=20))
        # fig.update_layout(hoverlabel=dict(font_size=16))

        # # Create graphJSON
        # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        # Setup AWS clients and locations
        s3_client = boto3.client('s3')
        s3_bucket = 's3bucket-comm-json'
        
        path = f'commodities/{commodity}.json'
        
        obj = s3_client.get_object(Bucket=s3_bucket, Key=path)
        file_content = obj['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        return jsonify(file_content)
    except Exception as e:
        return jsonify(error=str(e), stack_trace=traceback.format_exc()), 500