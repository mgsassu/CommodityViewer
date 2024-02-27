#!/usr/bin/env python
import pandas as pd
import datetime as dt
from datetime import timedelta
import sys
import os
import numpy as np
import glob

import openpyxl
import plotly.express as px

def main():
    # Timestamp for file output
    t1 = dt.datetime.now()
    t2 = str(t1.replace(microsecond=0))
    t3 = t2.replace(":","-")
    t4 = t3.replace(" ","_")
    dt_now_str = str(t4)
    
    # Load files
    path = "./OutputFiles_Reduced/*.xlsx"
    files = glob.glob(path)
    
    data = {}
    commodities = []
    for filepath in files:
        df = pd.read_excel(filepath)
        df['Date'] = pd.to_datetime(df['Report_Date_as_MM_DD_YYYY'])
        df = df.sort_values(by='Report_Date_as_MM_DD_YYYY')
        
        filex = os.path.basename(filepath)
        commodity = filex.split('.')[0]
        
        data[commodity] = df.to_dict(orient='records')
        commodities.append(commodity)

        fig = px.line(df, x = 'Date', y = df.columns[1:], title = f"{commodity} {dt_now_str}", markers=True)

        ### Save to html
        fig.write_html(f"./OutputFiles_Reduced/{commodity} {dt_now_str}.html")

        ## Save to image
        #fig.write_image(f"./OutputFiles_Reduced/{commodity} {dt_now_str}.png", height=1500, width=2100)


    print("Program Execution Complete.")

if __name__ == "__main__":
    main()