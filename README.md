# CommodityViewer
This is a simple Flask/Python web application that graphs commodity futures and options price data. 

Commodity data comes from the Disaggregated Futures-and-Options Combined Reports at https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm. 

The CommodityParser takes the individual historical files for each year, converts them into Plotly graphs, then converts the graph to JSON format and stores the JSON data in S3 in AWS. 

The CommodityViewer is the code for the webpage itself. It pulls the Plotly JSON data from S3, and displays it in a Plotly graph. 
