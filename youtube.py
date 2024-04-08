# Importing required packages ................................................................................

import googleapiclient.discovery
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Text, BigInteger, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import streamlit as st
from streamlit_option_menu import option_menu
import time
import plotly.express as px


Base = declarative_base()

# Connecting API Key .........................................................................................
def api_connection():
    api_key = 'AIzaSyAxLRwc8a8ASTDO9CtcnHtUbEiHiYdyD0A'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)
    return youtube

youtube = api_connection()

# Define SQLAlchemy engine and session ....................................................................... 
engine = create_engine('mysql+pymysql://root:Narendran18*@localhost:3306/ytproject')
Session = sessionmaker(bind=engine)
session = Session()

# Define ChannelTable class for Channel_Table ................................................................
class ChannelTable(Base):
    __tablename__ = 'Channel_Table'

    id = Column(Integer, primary_key=True)
    Channel_Name = Column(String(255))
    Channel_Id = Column(String(255))
    Subscribers = Column(BigInteger)
    View_Count = Column(BigInteger)
    Playlist_id = Column(String(100))
    Total_videos = Column(Integer)
    Description = Column(Text)

# Define VideozTable class for Videos_Table .................................................................

class video_table(Base):
    __tablename__ = 'video_table'

    id = Column(Integer, primary_key=True)
    Channel_Name = Column(String(255))
    Channel_Id = Column(String(255))
    Video_id = Column(String(255))
    Title = Column(String(255))
    Views = Column(BigInteger)
    Likes = Column(BigInteger)
    Comments = Column(BigInteger)
    Thumbnail = Column(String(255))
    published_at = Column(String(100))
    Duration = Column(String(100))
    Description = Column(Text)


# Define CommentTable class for comment_table ................................................................
class CommentTable(Base):
    __tablename__ = 'comment_table'

    id = Column(Integer, primary_key=True)
    comment_Id = Column(String(255))
    video_Id = Column(String(255))
    comment_Text = Column(Text)
    comment_Author = Column(String(255))
    comment_Published = Column(String(255))

# Create tables ..............................................................................................
Base.metadata.create_all(engine)

# Getting Channel Details and Inserting into table ...........................................................

def get_channel_details(channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()

    for item in response.get('items', []):
        data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_Id': channel_id,
            'Subscribers': int(item['statistics']['subscriberCount']),
            'View_Count': int(item['statistics']['viewCount']),
            'Playlist_id': item['contentDetails']['relatedPlaylists']['uploads'],
            'Total_videos': int(item['statistics']['videoCount']),
            'Description': item['snippet']['description']
        }
        # Create a new ChannelTable object and add it to the session
        channel_details = ChannelTable(**data)
        session.add(channel_details)

    # Commit the session
    session.commit()

    return data

# Getting Video Ids ..........................................................................................

def get_channel_videos(channel_id):
    video_ids = []

    request = youtube.channels().list(
                   id=channel_id,
                   part='contentDetails')
    response1 = request.execute()
    playlist_id = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id,
                                               part='snippet',
                                               maxResults=50,
                                               pageToken=next_page_token)
        response2 = request.execute()
  
        for item in response2.get('items', []):
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response2.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_ids

# Getting Video Details and Inserting into table ...........................................................

def get_video_details(video_ids):
    video_data = []
    
    try:
        for video_id in video_ids:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()

            for item in response.get('items', []):
                data = {
                    'Channel_Name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_id': item['id'],
                    'Title': item['snippet']['title'],
                    'Views': int(item['statistics']['viewCount']),
                    'Likes': int(item['statistics']['likeCount']),
                    'Comments': int(item['statistics']['commentCount']),
                    'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                    'published_at': item['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                    'Duration': item['contentDetails']['duration'].replace('PT', ' '),
                    'Description': item['snippet']['description']
                }
                video_data.append(data)

                # Create a new VideosTable object and add it to the session
                video_details = video_table(**data)
                session.add(video_details)

        # Commit the session
        session.commit()
                
    except Exception as e:
        print("Error:", e)            
    return video_data

# Getting Comment Details and Inserting into table ...........................................................

def get_comments_details(video_ids):
    comment_data = []
    
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    'comment_Id': item['snippet']['topLevelComment']['id'],
                    'video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_Published': item['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T', ' ').replace('Z', '')
                }
                comment_data.append(data)

                # Create a new CommentTable object and add it to the session
                comment_details = CommentTable(**data)
                session.add(comment_details)

        # Commit the session
        session.commit()
                
    except Exception as e:
        print("Error:", e)            
    return comment_data

# Merging all three functions into one function .............................................................

def channel_info(channel_id):
    try:
        channel_details = get_channel_details(channel_id)
        print("Channel details:", channel_details)
        
        video_ids = get_channel_videos(channel_id)
        print("Video IDs:", video_ids)
        
        video_details = get_video_details(video_ids)
        print("Video details:", video_details)
        
        comment_details = get_comments_details(video_ids)
        print("Comment details:", comment_details)

        if channel_details and video_details and comment_details:
            channel_df = pd.DataFrame([channel_details])
            video_df = pd.DataFrame(video_details)
            comment_df = pd.DataFrame(comment_details)

            return {
                "channel_details": channel_df,
                "video_details": video_df,
                "comment_details": comment_df,
            }
        else:
            return None
    except Exception as e:
        print("Error:", e)
        return None

# Streamlit Code .............................................................................................

with st.sidebar:
    option = option_menu("Menu",
                    ['Project Description 📝','Collect & Store','Querys 🧐',' Project Summary 🗒️'])
            
if option == "Project Description 📝":
        st.title(''':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]''')
        st.write("#    ")
        st.subheader('Vision of the Project :')
        st.write("The goal of this project is to create a Streamlit application that interacts with the YouTube Data API to gather information about YouTube channels. This information will then be processed and stored in a SQL data warehouse. Let's break down the key components and processes involved in this project:")
        st.subheader('YouTube Data API Integration :')
        st.write("The application will use the YouTube Data API to fetch data about YouTube channels. This API allows developers to retrieve information such as channel details, video statistics, and comments from YouTube.")
        st.subheader('Streamlit Application Development :')
        st.write("Streamlit is a Python library that allows developers to create interactive web applications easily. In this project, a Streamlit application will be developed to provide a user-friendly interface for users to enter a YouTube channel ID. The application will then use this ID to fetch channel information from the YouTube Data API.")
        st.subheader('Data Processing :')
        st.write("The data obtained from the YouTube Data API may need to be processed before being stored in the SQL data warehouse. This processing may involve tasks such as data cleaning, normalization, and transformation to ensure that the data is in a suitable format for storage and analysis.")
        st.subheader('SQL Data Warehouse :')
        st.write("A SQL data warehouse will be used to store the collected data from the YouTube channels. SQL databases are commonly used for data storage and retrieval due to their relational nature and query capabilities. The warehouse will provide a centralized repository for the gathered information, allowing for efficient storage and retrieval.")
        st.subheader('Search Functionality :')
        st.write("The Streamlit application will include various search functionalities to retrieve data from the SQL data warehouse. Users may be able to search for channels based on specific criteria such as channel name, subscriber count, video views, upload frequency, etc. The application will execute SQL queries against the warehouse to fetch and display the relevant information to the user.")

if option == ("Collect & Store"):
                
        st.markdown("#    ")
        st.write("### ENTER THE YOUTUBE CHANNEL ID ")
        channel_id = st.text_input("enter here below")
        
        if st.button('Collect & Store'):
                progress_text="Collecting Data , Please wait for a while..."
                my_bar=st.progress(0, text=progress_text)
  
                for percent_complete in range(100):
                    time.sleep(0)
                    my_bar.progress(percent_complete + 1, text=progress_text)
                    time.sleep(1)
                    my_bar.empty()
                    

                details = channel_info(channel_id)
                st.subheader('Channel Data')
                st.write(details["channel_details"])

                st.subheader('Video Data')
                st.write(details["video_details"])

                st.subheader('Comment Data')
                st.write(details["comment_details"])

if option == ("Querys 🧐"):
    Questions = st.selectbox("Select your Questions",
                                ["Choose your Questions 🤔",
                                "1. Names of all the videos and their corresponding channels",
                                "2. Channels with most number of videos",
                                "3. Top 10 most viewed videos and their respective channels",
                                "4. Number of Comments and the Video names",
                                "5. Videos with highest number of likes and their channel name",
                                "6. Total number of likes and their videos name",
                                "7. Total number of views and their channel name",
                                "8. Names of all the channels that have published videos in the year 2022",
                                "9. The average duration of all videos in each channel,their channel names",
                                "10. Videos with the highest number of comments, and their corresponding channel names"],
                                index  = 0)
    
    
    if Questions == "1. Names of all the videos and their corresponding channels":
        query1 = text('''select title as videos, channel_name as channelname from ytproject.video_table''')
        result = session.execute(query1)
        df = pd.DataFrame(result.fetchall(), columns=['videos', 'channelname'])
        st.write(df)

    elif Questions == "2. Channels with most number of videos":
        query2 = text('''select channel_name as channelname, total_videos as no_of_videos from ytproject.channel_table
                    order by total_videos  desc''')
        result = session.execute(query2)
        df2 = pd.DataFrame(result.fetchall(), columns = ['channel_name','total_videos'])
        st.write(df2)
        #print(df2.columns)
        fig = px.bar(df2, x='channel_name', y='total_videos',
        labels={'Channel_Name': 'Channel Name', 'Video_Count': 'No of Videos'},
        title='Channels with most number of videos')
        fig.update_layout(xaxis_tickangle=360)
        st.plotly_chart(fig)

    elif Questions == "3. Top 10 most viewed videos and their respective channels":
        query3 = text('''select Title, Channel_Name, Views from ytproject.video_table
                    order by Views desc limit 10''')
        result = session.execute(query3)
        df3 = pd.DataFrame(result.fetchall(), columns = ['Title','Channel_Name','Views'])
        st.write(df3)

    elif Questions == "4. Number of Comments and the Video names":
        query4 = text('''select channel_name, Title,  comments from ytproject.video_table''')
        result = session.execute(query4)
        df4 = pd.DataFrame(result.fetchall(), columns = ['Channel_Name','Ttile','Comments'])
        st.write(df4)

    elif Questions == "5. Videos with highest number of likes and their channel name":
        query5 = text('''select title as videotitle, channel_name as channelname, likes as likes_count from ytproject.video_table
                    where likes is not null
                    order by likes desc''')
        result = session.execute(query5)
        df5 = pd.DataFrame(result.fetchall(), columns = ['videotitle','channel_name','likes_count'])
        st.write(df5)
        fig = px.bar(df5, x='videotitle', y='likes_count', color='channel_name',
                        labels={'Title': 'Video Title', 'Likes': 'Number of Likes'},
                        title='Videos with the Highest Number of Likes and Their Corresponding Channels')
        fig.update_layout(xaxis_tickangle=90) 
        st.plotly_chart(fig)

    elif Questions == "6. Total number of likes and their videos name":
        query6 = text('''select title as videotitle, likes as no_of_likes from ytproject.video_table''')
        result = session.execute(query6)
        df6 = pd.DataFrame(result.fetchall(), columns = ['Videos_Title','Likes_Count'])
        st.write(df6)
        fig = px.line(df6, x='Videos_Title', y='Likes_Count',
                 labels={'Title': 'Title', 'Likes': 'Likes'},
                 title='Total Number of Likes for Each Video')
        st.plotly_chart(fig)

    elif Questions == "7. Total number of views and their channel name":
        query7 = text('''select channel_name as channelname, view_count as total_views from ytproject.channel_table''')
        result = session.execute(query7)
        df7 = pd.DataFrame(result.fetchall(), columns = ['Channel_Name','Total_Views'])
        st.write(df7)
        fig = px.bar(df7, x='Channel_Name', y='Total_Views',
                             color='Channel_Name',
                 labels={'Channel_Name': 'Channel Name', 'Views': 'Total Number of Views'},
                 title='Total Number of Views for Each Channel')
        st.plotly_chart(fig)

    elif Questions == "8. Names of all the channels that have published videos in the year 2022":
        query8 = text('''SELECT Title AS video_title, published_at AS release_date, channel_name AS channelname 
                         FROM ytproject.video_table 
                         WHERE EXTRACT(YEAR FROM published_at) = 2022''')     
        result = session.execute(query8)
        df8 = pd.DataFrame(result.fetchall(), columns=['video_title', 'release_date', 'channelname'])
        st.write(df8)

    elif Questions == "9. The average duration of all videos in each channel,their channel names":
        query9 = text('''select channel_name as channel_name, AVG(duration) as average_duration from ytproject.video_table
                        group by channel_name''')       
        result = session.execute(query9)
        df9 = pd.DataFrame(result.fetchall(), columns = ['channel_name','average_duration'])
        st.write(df9)
        fig = px.bar(df9, x='channel_name', y='average_duration',
                 labels={'Channel_Name': 'Channel Name', 'Average_Duration': 'Average Duration'},
                 title='Average Duration of Videos in Each Channel')
        st.plotly_chart(fig)

    elif Questions == "10. Videos with the highest number of comments, and their corresponding channel names":
        query10 = text('''select channel_name as channelname, title as video_title, comments as no_of_comments from ytproject.video_table
                     where comments is not null
                     order by comments desc''')
        result = session.execute(query10)
        df10 = pd.DataFrame(result.fetchall(), columns = ['channelname','video_title','comments'])
        st.write(df10)
        fig = px.bar(df10, x='video_title', y='comments', color='channelname',
             title='Number of Comments for Each Video', 
             labels={'video_title': 'Video Title', 'comments': 'Number of Comments', 'channelname': 'Channel Name'})
        st.plotly_chart(fig)

if option == "Project Summary 🗒️":
    st.subheader("Project Summary 🗒️")
    st.write("This project aims to create a user-friendly Streamlit application that harnesses the power of the YouTube Data API to collect comprehensive information about YouTube channels. Through seamless integration with the API, users can effortlessly input a channel ID and access an array of data, including channel details, video statistics, and comments. The gathered data undergoes meticulous processing to ensure consistency and relevance before being securely stored in a SQL data warehouse. This warehouse serves as a centralized repository, facilitating efficient storage and retrieval of the accumulated YouTube channel information.") 
    st.write("# ")  
    st.write("Moreover, the Streamlit application offers users diverse search functionalities, empowering them to query the SQL database based on specific criteria such as channel name, subscriber count, upload frequency, and more. This enables users to delve deep into the YouTube ecosystem, explore trends, and perform insightful analyses. By providing a seamless interface for both data retrieval and analysis, the application caters to users' needs for convenience and efficiency. Ultimately, this project not only facilitates access to valuable YouTube channel insights but also empowers users to make informed decisions and discoveries within the dynamic landscape of online video content.")
