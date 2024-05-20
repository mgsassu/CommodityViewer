#!/usr/bin/env python
import pandas as pd
import datetime as dt
from datetime import timedelta
import sys
import os
import numpy as np
import glob
import json

import openpyxl
import plotly.express as px
import plotly.utils as pu

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
        
        
        filex = os.path.basename(filepath)
        commodity = filex.split('.')[0]
        data[commodity] = df.to_dict(orient='records')
        commodities.append(commodity)

        # Create the figure
        fig = px.line(df, x = 'Date', y = df.columns[1:], hover_data={"Date": "|%B %d, %Y"}, title = f"{commodity}: Most recent data from {str(df['Date'].iloc[-1].date())}", markers=True)
        fig.update_layout(font=dict(size=20))
        fig.update_layout(hoverlabel=dict(font_size=16))
        
        ### Save to html
        # fig.write_html(f"./OutputFiles_Reduced/{commodity} {dt_now_str}.html")

        ## Save to image
        #fig.write_image(f"./OutputFiles_Reduced/{commodity} {dt_now_str}.png", height=1500, width=2100)

        # Create graphJSON and save
        # graphJSON = json.dumps(fig, cls=pu.PlotlyJSONEncoder)
        jsonFileName = f"./OutputFiles_Reduced/{commodity} {dt_now_str}.json"
        with open(jsonFileName, 'w', encoding='utf-8') as f:
            f.write(json.dumps(fig, cls=pu.PlotlyJSONEncoder))
        print(f"Finished writing {jsonFileName}. Continuing...")

    print("Program Execution Complete.")

if __name__ == "__main__":
    main()