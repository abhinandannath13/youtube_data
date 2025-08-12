import pyodbc
import json
import requests
import time
import pandas as pd
import os

from dotenv import load_dotenv
load_dotenv()



df = pd.read_csv("youtube_trending_videos.csv")




server = os.getenv("AZURE_SQL_SERVER")          
database = os.getenv("AZURE_SQL_DATABASE")       

# Use Managed Identity authentication 
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};DATABASE={database};Authentication=ActiveDirectoryMsi"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()


insert_sql = '''
    INSERT INTO trending_videos (
        channelid, channel_title, video_category, video_id, video_publishedat,
        video_title, video_views, video_likes, video_comments, video_duration,
        video_monetize, region_code, region_name, trending_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

data_tuples = list(df.itertuples(index=False, name=None))
cursor.executemany(insert_sql, data_tuples)
conn.commit()

print(f"✅ Inserted {len(data_tuples)} records successfully.")

if cursor:
    cursor.close()
if conn:
    conn.close()
