import pyodbc
import json
import requests
import time
import pandas as pd
import os

from dotenv import load_dotenv
load_dotenv()

# def scrape_videos():
#     region_url = "https://www.googleapis.com/youtube/v3/i18nRegions"
#     vid_url = "https://www.googleapis.com/youtube/v3/videos"
#     api_key = api_key

#     try:
#         region_param = {'part': 'snippet', 'key': api_key}
#         region_response = requests.get(region_url, params=region_param)
#         region_data = region_response.json()

#         if 'items' not in region_data:
#             print("Error: Unexpected regions response structure:")
#             return json.dumps([])
#     except Exception as e:
#         print(f"Failed to fetch regions: {str(e)}")
#         return json.dumps([])

#     all_videos = []

#     for i in region_data['items']:
#         region_code = i['snippet']['gl']
#         region_name = i['snippet']['name']

#         params = {
#             'part': 'snippet,statistics,contentDetails',
#             'chart': 'mostPopular',
#             'regionCode': region_code,
#             'maxResults': 50,
#             'key': api_key
#         }

#         page_count = 0
#         video_count = 0
#         print(f"Processing region : {region_name} ({region_code})")

#         while True:
#             page_count += 1
#             try:
#                 response = requests.get(vid_url, params=params)
#                 data = response.json()

#                 if 'items' not in data:
#                     print("  No videos found in response")
#                     break

#                 current_page_videos = len(data['items'])
#                 video_count += current_page_videos
#                 print(f"  Found {current_page_videos} videos (Total: {video_count})")

#                 for vid_data in data['items']:
#                     try:
#                         all_videos.append({
#                             'channelid': vid_data["snippet"]["channelId"],
#                             'channel_title': vid_data["snippet"]["channelTitle"],
#                             'video_category': vid_data["snippet"]["categoryId"],
#                             'video_id': vid_data["id"],
#                             'video_publishedat': vid_data["snippet"]["publishedAt"],
#                             'video_title': vid_data["snippet"]["title"],
#                             'video_views': vid_data["statistics"].get("viewCount", "0"),
#                             'video_likes': vid_data["statistics"].get("likeCount", "0"),
#                             'video_comments': vid_data["statistics"].get("commentCount", "0"),
#                             'video_duration': vid_data["contentDetails"]["duration"],
#                             'video_monetize': vid_data["contentDetails"]["licensedContent"],
#                             'region_code': region_code,
#                             'region_name': region_name,
#                             'trending_date': time.strftime('%Y-%m-%d %H:%M:%S')
#                         })
#                     except KeyError as e:
#                         print(f"  Skipping video due to missing field: {str(e)}")
#                         continue

#                 if 'nextPageToken' not in data:
#                     break

#                 params['pageToken'] = data['nextPageToken']
#                 time.sleep(1)  # Rate limiting

#             except Exception as e:
#                 print(f"  Error processing page {page_count}: {str(e)}")
#                 break

#         print(f"Completed {region_name} - Total videos: {video_count}\n")
#         time.sleep(2)  # Longer pause between regions

#     df = pd.DataFrame(all_videos)
#     return df

# df = scrape_videos()

# df.to_json("youtube_trending_videos.json", orient="records", indent=2)
# df.to_csv("youtube_trending_videos.csv", index=False, encoding="utf-8")


df = pd.read_csv("youtube_trending_videos.csv")




# Azure SQL Database connection parameters
server = os.getenv("AZURE_SQL_SERVER", "tcp:yourserver.database.windows.net")
database = os.getenv("AZURE_SQL_DATABASE", "yourdbname")
username = os.getenv("AZURE_SQL_USERNAME", "yourusername")
password = os.getenv("AZURE_SQL_PASSWORD", "yourpassword")
driver= '{ODBC Driver 17 for SQL Server}'

# Connection string
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;'

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
