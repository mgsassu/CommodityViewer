import pandas as pd
import numpy as np
import datetime as dt
import glob

import boto3

def main():
    # Current time
    t1 = dt.datetime.now()
    t2 = str(t1.replace(microsecond=0))
    t3 = t2.replace(":","-")
    t4 = t3.replace(" ","_")
    dt_now_str = str(t4)
    
    # Commodities dict
    commodities_dict = {"Cotton": ["COTTON NO. 2 - ICE FUTURES U.S.", "COTTON NO. 2 - NEW YORK BOARD OF TRADE"],
                        "WheatSRW": ["WHEAT-SRW - CHICAGO BOARD OF TRADE", "WHEAT - CHICAGO BOARD OF TRADE"],
                        "WheatHRW": ["WHEAT-HRW - CHICAGO BOARD OF TRADE", "WHEAT - KANSAS CITY BOARD OF TRADE"],
                        "WheatHRSpring": ["WHEAT-HRSpring - MINNEAPOLIS GRAIN EXCHANGE", "WHEAT - MINNEAPOLIS GRAIN EXCHANGE"],
                        "Corn": ["CORN - CHICAGO BOARD OF TRADE"],
                        "Soybeans": ["SOYBEANS - CHICAGO BOARD OF TRADE"],
                        "SoybeanOil": ["SOYBEAN OIL - CHICAGO BOARD OF TRADE"],
                        "SoybeanMeal": ["SOYBEAN MEAL - CHICAGO BOARD OF TRADE"],
                        "RoughRice": ["ROUGH RICE - CHICAGO BOARD OF TRADE"],
                        "FrznOJ": ["FRZN CONCENTRATED ORANGE JUICE - ICE FUTURES U.S."],
                        "Butter": ["BUTTER (CASH SETTLED) - CHICAGO MERCANTILE EXCHANGE"],
                        "MilkClassIII": ["MILK, Class III - CHICAGO MERCANTILE EXCHANGE"],
                        "NonFatDryMilk": ["NON FAT DRY MILK - CHICAGO MERCANTILE EXCHANGE"],
                        "CMEMilkIV": ["CME MILK IV - CHICAGO MERCANTILE EXCHANGE"],
                        "Cheese": ["CHEESE (CASH-SETTLED) - CHICAGO MERCANTILE EXCHANGE"],
                        "LeanHogs": ["LEAN HOGS - CHICAGO MERCANTILE EXCHANGE"],
                        "LiveCattle": ["LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE"],
                        "FeederCattle": ["FEEDER CATTLE - CHICAGO MERCANTILE EXCHANGE"],
                        "Cocoa": ["COCOA - ICE FUTURES U.S.", "COCOA - NEW YORK BOARD OF TRADE"],
                        "Sugar": ["SUGAR NO. 11 - ICE FUTURES U.S.", "SUGAR NO. 11 - NEW YORK BOARD OF TRADE"],
                        "Coffee": ["COFFEE C - ICE FUTURES U.S.", "COFFEE C - NEW YORK BOARD OF TRADE"],
                        "PalmOil": ["USD Malaysian Crude Palm Oil C - CHICAGO MERCANTILE EXCHANGE"],
                        "Canola": ["CANOLA - ICE FUTURES U.S."]}
    
    # # Dict of pandas dataframes, one for each of the keys in the above dict
    # dataframe_dict = {}
    # for key in commodities_dict.keys():
    #     dataframe_dict.append()
    
    # Get all files
    path = "./DataFiles/*.xls"
    files = glob.glob(path)
    
    # Loop through files, append to the appropriate pandas dataframe
    final_dict = {}
    for file in files:
        # read in the file
        print(f"Reading in {file}...")
        df_cftc = pd.read_excel(file)
        # Loop through each of our commodity keys
        for key in commodities_dict.keys():
            print(f"Processing {file} - {key}")
            # Parse the cftc df into the value associated with each key. Note that there may be more than 1 value that we need to parse and append
            for i in range(0, len(commodities_dict[key])):
                # Do the parsing
                df_temp = df_cftc[df_cftc["Market_and_Exchange_Names"] == commodities_dict[key][i]]
                # If we don't have any data, just continue
                if len(df_temp) == 0:
                    continue
                # Concat, or make a new dataframe if there isn't one, or this is our first loop
                if key in final_dict.keys():
                    final_dict[key] = pd.concat([final_dict[key], df_temp])
                else:
                     final_dict[key] = df_temp.copy()
                     
    # loop through the final dict. Create new columns and export
    for key in final_dict.keys():
        df = final_dict[key]
        df['Prod_Net'] = df.apply(lambda row: row['Prod_Merc_Positions_Long_ALL'] - row['Prod_Merc_Positions_Short_ALL'], axis=1)
        df['Swap_Net'] = df.apply(lambda row: row['Swap_Positions_Long_All'] - row['Swap__Positions_Short_All'], axis=1)
        df['MM_Net'] = df.apply(lambda row: row['M_Money_Positions_Long_ALL'] - row['M_Money_Positions_Short_ALL'], axis=1)
        df['Other_Reportable_Net'] = df.apply(lambda row: row['Other_Rept_Positions_Long_ALL'] - row['Other_Rept_Positions_Short_ALL'], axis=1)
        df['Non_Reportable_Net'] = df.apply(lambda row: row['NonRept_Positions_Long_All'] - row['NonRept_Positions_Short_All'], axis=1)
        
        df['Date'] = pd.to_datetime(df['Report_Date_as_MM_DD_YYYY'])
        df = df.sort_values(by='Date')
        
        # Make a new dataframe of just the desired columns. Apparently filter makes a new copy in memory, so we're using that one. 
        df_filtered = df.filter(['Date','Prod_Net','Swap_Net','MM_Net','Other_Reportable_Net','Non_Reportable_Net'])

        df_filtered.to_excel(f"./OutputFiles_Reduced/{key}.xlsx", index=False)
        df_filtered.to_csv(f"./OutputFiles_Reduced/{key}.csv", index=False)


if __name__ == "__main__":
    main()