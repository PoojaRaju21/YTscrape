import streamlit as st
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
from bson import ObjectId
import json
import mysql.connector
import pandas as pd
import traceback
from sqlalchemy import create_engine

# Connect to MongoDB Atlas
atlas_username = 'Pooja_30'
atlas_password = 'ZsMMa78AcoQO7Mwe'
atlas_cluster = 'cluster0.m23dqpy.mongodb.net'
client = MongoClient(f"mongodb+srv://{atlas_username}:{atlas_password}@{atlas_cluster}/")
db = client['youtube_data1']
collection = db['channel_data1']

# Connect to MySQL
mysql_username = 'venba'
mysql_password = '1111'
mysql_host = 'localhost'
mysql_database = 'youtube_data'
mysql_auth_plugin = 'mysql_native_password'

# Set page layout to center content
st.set_page_config(layout="centered")

# Apply red color to the title using Markdown
st.markdown("<h1 style='text-align: center; color: brown;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)


# Display input field for YouTube channel IDs
channel_ids_input = st.text_input("Enter YouTube Channel IDs (separated by commas)")

# Limit the number of entered channel IDs to 3
max_channel_ids = 10
channel_ids_list = [id.strip() for id in channel_ids_input.split(",") if id.strip()][:max_channel_ids]

# Initialize YouTube Data API client
youtube = build('youtube', 'v3', developerKey='AIzaSyAqMPRcS0xdTVCmqegQL79xXah3AHl-F70')

# Store data in MongoDB Atlas
def store_data_in_mongodb(data):
    collection.insert_one(data)

# Retrieve data from MongoDB Atlas
def retrieve_data_from_mongodb(channel_id):
    retrieved_data = collection.find_one({'Channel_Name.Channel_Id': channel_id})
    return retrieved_data


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

def get_playlist_videos(youtube, playlist_id, max_results=10):
    videos = []
    request = youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=playlist_id,
        maxResults=max_results
    )
    response = request.execute()

    video_ids = [item['contentDetails']['videoId'] for item in response['items']]

    video_request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=','.join(video_ids)
    )
    video_response = video_request.execute()

    videos.extend(video_response['items'])

    return videos

def parse_duration(duration):
    duration_str = ""
    hours = 0
    minutes = 0
    seconds = 0

    # Remove 'PT' prefix from duration
    duration = duration[2:]

    # Check if hours, minutes, and/or seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.index("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index+1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index+1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # Format the duration string
    if hours > 0:
        duration_str += f"{hours}h "
    if minutes > 0:
        duration_str += f"{minutes}m "
    if seconds > 0:
        duration_str += f"{seconds}s"

    return duration_str.strip()

# Main program logic
if len(channel_ids_list) > max_channel_ids:
    st.warning(f"You are reaching the maximum limit of {max_channel_ids} channel IDs. Please enter up to {max_channel_ids} channel IDs.")
elif st.button("Retrieve and Store Channel Data", key="retrieve_store_data"):
    try:
        channel_ids_list = [id.strip() for id in channel_ids_input.split(",") if id.strip()][:max_channel_ids]
        for channel_id in channel_ids_list:
            # Check if the data for the channel already exists in MongoDB
            existing_data = retrieve_data_from_mongodb(channel_id)

            if existing_data:
                st.warning(f"Data already exists in MongoDB Atlas for Channel ID: {channel_id}")
                continue  # Skip storing data for this channel

            # Make API request to get channel data
            request = youtube.channels().list(
                part='snippet,statistics,contentDetails',
                id=channel_id
            )
            response = request.execute()

            if 'items' in response:
                channel_data = response['items'][0]
                snippet = channel_data['snippet']
                statistics = channel_data.get('statistics', {})
                content_details = channel_data.get('contentDetails', {})
                related_playlists = content_details.get('relatedPlaylists', {})

                # Extract relevant data
                data = {
                    'Channel_Name': {
                        'Channel_Name': snippet.get('title', ''),
                        'Channel_Id': channel_id,
                        'Subscription_Count': int(statistics.get('subscriberCount', 0)),
                        'Channel_Views': int(statistics.get('viewCount', 0)),
                        'Channel_Description': snippet.get('description', ''),
                        'Playlist_Id': related_playlists.get('uploads', '')
                    }
                }

                # Retrieve playlist data
                playlists_request = youtube.playlists().list(
                    part='snippet',
                    channelId=channel_id,
                    maxResults=10  # Specify the number of playlists to retrieve
                )
                playlists_response = playlists_request.execute()

                playlist_data = []
                for item in playlists_response.get('items', []):
                    playlist_id = item['id']
                    playlist_name = item['snippet']['title']
                    playlist_data.append({'playlist_id': playlist_id, 'playlist_name': playlist_name})

                data['Channel_Name']['Playlists'] = playlist_data

                # Iterate through each playlist and retrieve limited number of videos
                max_videos_per_playlist = 25  # Specify the maximum number of videos per playlist

                for playlist in playlist_data:
                    playlist_id = playlist['playlist_id']
                    videos = get_playlist_videos(youtube, playlist_id, max_results=max_videos_per_playlist)

                    playlist_videos = []  # Create a list to store video data for this playlist

                    for video in videos:
                        video_id = video['id']
                        video_data = {
                            'Video_Id': video_id,
                            'Video_Name': video['snippet'].get('title', ''),
                            'Video_Description': video['snippet'].get('description', ''),
                            'Tags': video['snippet'].get('tags', []),
                            'PublishedAt': video['snippet'].get('publishedAt', ''),
                            'View_Count': int(video['statistics'].get('viewCount', 0)),
                            'Like_Count': int(video['statistics'].get('likeCount', 0)),
                            'Dislike_Count': int(video['statistics'].get('dislikeCount', 0)),
                            'Favorite_Count': int(video['statistics'].get('favoriteCount', 0)),
                            'Comment_Count': int(video['statistics'].get('commentCount', 0)),
                            'Duration': parse_duration(video['contentDetails'].get('duration', '')),
                            'Thumbnail': video['snippet'].get('thumbnails', {}).get('default', {}).get('url', ''),
                            'Caption_Status': video['snippet'].get('localized', {}).get('localized', 'Not Available'),
                            'Comments': []
                        }

                        # Retrieve comments for the video
                        comments_request = youtube.commentThreads().list(
                            part='snippet',
                            videoId=video_id,
                            maxResults= 25  # Specify the number of comments to retrieve
                        )
                        comments_response = comments_request.execute()

                        for item in comments_response['items']:
                            comment = item['snippet']['topLevelComment']['snippet']
                            comment_data = {
                                'Comment_Id': comment.get('id', ''),
                                'Comment_Text': comment.get('textDisplay', ''),
                                'Comment_Author': comment.get('authorDisplayName', ''),
                                'Comment_PublishedAt': comment.get('publishedAt', '')
                            }
                            video_data['Comments'].append(comment_data)

                        playlist_videos.append(video_data)  # Append video data to the playlist's list

                    # Add the playlist's video data to the main data dictionary
                    data[playlist_id] = playlist_videos

                # Store data in MongoDB Atlas
                store_data_in_mongodb(data)
                # st.success(f"Data stored successfully in MongoDB Atlas")
                st.success(f"Data stored successfully in MongoDB Atlas for Channel ID: {channel_id}")

    except Exception as e:
        st.error(f"Error retrieving channel data: {str(e)}")

# Retrieve and display data from MongoDB Atlas
if st.button("Retrieve Data from MongoDB Atlas",key="retrieve_mongodb_data"):
    try:
        channel_ids_list = [id.strip() for id in channel_ids_input.split(",") if id.strip()][:max_channel_ids]
        for channel_id in channel_ids_list:
            retrieved_data = retrieve_data_from_mongodb(channel_id)
            if retrieved_data:
                st.subheader(f"Retrieved Data for Channel ID: {channel_id}")
                json_data = json.dumps(retrieved_data, indent=4, cls=CustomJSONEncoder)
                st.code(json_data, language='json')
            else:
                st.warning(f"Data not found in MongoDB Atlas for Channel ID: {channel_id}")

    except Exception as e:
        st.error(f"Error retrieving data from MongoDB Atlas: {str(e)}")
        traceback.print_exc()  # Print detailed error traceback for debugging

# Create tables in MySQL
def create_mysql_tables(mysql_cursor):
    # Drop tables if they exist
    mysql_cursor.execute("DROP TABLE IF EXISTS comment_data")
    mysql_cursor.execute("DROP TABLE IF EXISTS video_data")
    mysql_cursor.execute("DROP TABLE IF EXISTS playlist_data")
    mysql_cursor.execute("DROP TABLE IF EXISTS channel_data")

    # Create table for channel data
    mysql_cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_data (
            id INT PRIMARY KEY AUTO_INCREMENT,
            channel_name VARCHAR(255),
            channel_id VARCHAR(255),
            subscription_count BIGINT,
            channel_views BIGINT,
            channel_description TEXT,
            playlist_id VARCHAR(255),
            channel_data_json JSON,
            INDEX idx_channel_id (channel_id)
        )
    ''')

    # Create table for playlist data
    mysql_cursor.execute('''
           CREATE TABLE IF NOT EXISTS playlist_data (
               id INT PRIMARY KEY AUTO_INCREMENT,
               playlist_name VARCHAR(255),
               playlist_id VARCHAR(255),
               channel_id VARCHAR(255),
               INDEX idx_playlist_id (playlist_id),
               INDEX idx_channel_id (channel_id),
               FOREIGN KEY (channel_id) REFERENCES channel_data(channel_id)               
           )
       ''')

    # Create table for video data
    mysql_cursor.execute('''
        CREATE TABLE IF NOT EXISTS video_data (
            id INT PRIMARY KEY AUTO_INCREMENT,
            video_id VARCHAR(255),
            channel_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT,
            tags TEXT,
            published_at VARCHAR(255),
            view_count INT,
            like_count INT,
            dislike_count INT,
            favorite_count INT,
            comment_count INT,
            duration VARCHAR(255),
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255),
            INDEX idx_video_id (video_id), 
            FOREIGN KEY (channel_id) REFERENCES channel_data(channel_id)
        )
    ''')

    # Create table for comment data
    mysql_cursor.execute('''
        CREATE TABLE IF NOT EXISTS comment_data (
            id INT PRIMARY KEY AUTO_INCREMENT,
            video_id VARCHAR(255),
            comment_id VARCHAR(255),
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_published_at VARCHAR(255),
            FOREIGN KEY (video_id) REFERENCES video_data(video_id)
        )
    ''')

def migrate_all_data_to_mysql(channel_ids):
    try:
        mysql_connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_username,
            password=mysql_password,
            database=mysql_database
        )

        mysql_cursor = mysql_connection.cursor()
        # Create tables in MySQL
        create_mysql_tables(mysql_cursor)

        for channel_id in channel_ids:
            # Retrieve data from MongoDB
            retrieved_data = retrieve_data_from_mongodb(channel_id)

            if retrieved_data:
                # print(f"Retrieved data for Channel ID: {channel_id}")
                # Store channel data in MySQL
                channel_name = retrieved_data['Channel_Name']['Channel_Name']
                subscription_count = retrieved_data['Channel_Name']['Subscription_Count']
                channel_views = retrieved_data['Channel_Name']['Channel_Views']
                channel_description = retrieved_data['Channel_Name']['Channel_Description']
                playlist_id = retrieved_data['Channel_Name']['Playlist_Id']

                # Convert retrieved_data dictionary to JSON string
                channel_data_json = json.dumps(retrieved_data, cls=CustomJSONEncoder)

                mysql_cursor.execute(
                    "INSERT INTO channel_data (channel_name, channel_id, subscription_count, channel_views, channel_description, playlist_id, channel_data_json) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (channel_name, channel_id, subscription_count, channel_views, channel_description, playlist_id, channel_data_json)
                )

                # Commit the changes
                mysql_connection.commit()
                playlists = retrieved_data['Channel_Name'].get('Playlists', [])
                if playlists:
                    for playlist in playlists:
                        playlist_name = playlist['playlist_name']
                        playlist_id = playlist['playlist_id']

                        mysql_cursor.execute(
                            "INSERT INTO playlist_data (playlist_name, playlist_id, channel_id) VALUES (%s, %s, %s)",
                            (playlist_name, playlist_id, channel_id)
                        )

                        # Commit the changes
                        mysql_connection.commit()

                # Iterate through each playlist and insert playlist data into MySQL
                for playlist in retrieved_data['Channel_Name']['Playlists']:
                    playlist_id = playlist['playlist_id']

                    # Iterate through each video in the playlist
                    for video_data in retrieved_data.get(playlist_id, []):
                        video_id = video_data['Video_Id']
                        video_name = video_data['Video_Name']
                        video_description = video_data['Video_Description']
                        tags = ','.join(video_data.get('Tags', []))
                        published_at = video_data['PublishedAt']
                        view_count = video_data['View_Count']
                        like_count = video_data['Like_Count']
                        dislike_count = video_data['Dislike_Count']
                        favorite_count = video_data['Favorite_Count']
                        comment_count = video_data['Comment_Count']
                        duration = video_data['Duration']
                        thumbnail = video_data['Thumbnail']
                        caption_status = video_data['Caption_Status']

                        mysql_cursor.execute('''
                            INSERT INTO video_data
                            (channel_id, video_id, video_name, video_description, tags, published_at,
                            view_count, like_count, dislike_count, favorite_count, comment_count,
                            duration, thumbnail, caption_status)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (channel_id, video_id, video_name, video_description, tags, published_at,
                              view_count, like_count, dislike_count, favorite_count, comment_count,
                              duration, thumbnail, caption_status))

                        # Commit the changes
                        mysql_connection.commit()

                        # Iterate through comments and insert comment data into MySQL
                        for comment_data in video_data.get('Comments', []):
                            comment_id = comment_data['Comment_Id']
                            comment_text = comment_data['Comment_Text']
                            comment_author = comment_data['Comment_Author']
                            comment_published_at = comment_data['Comment_PublishedAt']

                            mysql_cursor.execute('''
                                INSERT INTO comment_data
                                (video_id, comment_id, comment_text, comment_author, comment_published_at)
                                VALUES
                                (%s, %s, %s, %s, %s)
                            ''', (video_id, comment_id, comment_text, comment_author, comment_published_at))

                            # Commit the changes
                            mysql_connection.commit()

                st.success(f"Data migrated successfully to MySQL for Channel ID: {channel_id}")
            else:
                st.warning(f"No data found in MongoDB Atlas for Channel ID: {channel_id}")

        # Close the cursor and connection
        mysql_cursor.close()
        mysql_connection.close()

    except Exception as e:
        st.error(f"Error migrating data: {str(e)}")



# Migrate data from MongoDB to MySQL for all retrieved channel IDs
if st.button("Migrate Data from MongoDB to MySQL",key="migrate_data"):
    channel_ids_list = [id.strip() for id in channel_ids_input.split(",") if id.strip()][:max_channel_ids]
    migrate_all_data_to_mysql(channel_ids_list)

# Connect to MySQL
mysql_username = 'root'
mysql_password = 'pooja'
mysql_host = 'localhost'
mysql_database = 'youtube_data'

# Apply red color to the title using Markdown
st.markdown("<h1 style='text-align: center; color: green;'>YouTube Data Analysis</h1>", unsafe_allow_html=True)

engine = create_engine(f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_database}")

# SQL Queries
sql_queries = {
    'All Videos and Their Corresponding Channels': '''
        SELECT v.video_name AS `Video Name`, c.channel_name AS `Channel Name`
        FROM video_data v
        JOIN channel_data c ON v.channel_id = c.channel_id
    ''',
    'Channels with the Most Number of Videos': '''
        SELECT c.channel_name AS `Channel Name`, COUNT(v.video_id) AS `Number of Videos`
        FROM channel_data c
        JOIN video_data v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name, c.channel_id
        ORDER BY COUNT(v.video_id) DESC
    ''',
    'Top 10 Most Viewed Videos': '''
        SELECT v.video_name AS `Video Name`, c.channel_name AS `Channel Name`, v.view_count AS `View Count`
        FROM video_data v
        JOIN channel_data c ON v.channel_id = c.channel_id
        ORDER BY v.view_count DESC
        LIMIT 10
    ''',
    'Number of Comments on Each Video': '''
        SELECT v.video_name AS `Video Name`, COUNT(c.comment_id) AS `Number of Comments`
        FROM video_data v
        JOIN comment_data c ON v.video_id = c.video_id
        GROUP BY v.video_id, v.video_name
    ''',
    'Videos with the Highest Number of Likes': '''
        SELECT v.video_name AS `Video Name`, c.channel_name AS `Channel Name`, v.like_count AS `Number of Likes`
        FROM video_data v
        JOIN channel_data c ON v.channel_id = c.channel_id
        ORDER BY v.like_count DESC
        LIMIT 10
    ''',
    'Total Number of Likes and Dislikes for Each Video': '''
        SELECT v.video_name AS `Video Name`, v.like_count AS `Number of Likes`, v.dislike_count AS `Number of Dislikes`
        FROM video_data v
    ''',
    # Total Number of Views for Each Channel
    'Total Number of Views for Each Channel': '''
        SELECT c.channel_name AS `Channel Name`, SUM(v.view_count) AS `Total Views`
        FROM channel_data c
        JOIN video_data v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name
    ''',
    'Select channels that published videos in 2022': '''
       SELECT DISTINCT c.channel_name AS `Channel Name`
       FROM channel_data c
      JOIN video_data v ON c.channel_id = v.channel_id
     WHERE YEAR(STR_TO_DATE(v.published_at, '%Y-%m-%dT%H:%i:%sZ')) = 2022
    ''',

    # Average Duration of Videos in Each Channel
    'Average Duration of Videos in Each Channel': '''
        SELECT c.channel_name AS `Channel Name`, AVG(v.duration) AS `Average Duration`
        FROM channel_data c
        JOIN video_data v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name, c.channel_id
    ''',

    # Videos with the Highest Number of Comments
    'Videos with the Highest Number of Comments': '''
        SELECT
            v.video_name AS `Video Name`,
            c.channel_name AS `Channel Name`,
            COUNT(IFNULL(co.comment_id, 0)) AS `Number of Comments`
        FROM
            video_data v
        JOIN
            channel_data c ON v.channel_id = c.channel_id
        LEFT JOIN
            comment_data co ON v.video_id = co.video_id
        GROUP BY
            v.video_id, v.video_name, c.channel_name
        ORDER BY
            COUNT(IFNULL(co.comment_id, 0)) DESC
        LIMIT 10
    '''
}

# Function to execute SQL query and display result as table
def execute_sql_query(query):
    try:
        # Read the SQL query result into a Pandas DataFrame using the provided engine
        df = pd.read_sql_query(query, engine)

        # Display the DataFrame as a Streamlit table
        st.table(df)

    except Exception as e:
        st.error(f"Error executing SQL query: {str(e)}")

# Display SQL query results
query_name = st.selectbox("Select Query", list(sql_queries.keys()))

if st.button("Execute SQL Query", key="execute_sql"):
    query = sql_queries.get(query_name)
    if query:
        execute_sql_query(query)
    else:
        st.warning("Please select a valid SQL query.")



