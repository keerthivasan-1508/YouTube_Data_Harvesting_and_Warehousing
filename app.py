from googleapiclient.discovery import build
import pymongo
import mysql.connector as sql
import pandas as pd
from datetime import datetime
import streamlit as st
import json


st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   )

st.title(":red[Youtube Data Harvesting and Warehousing]")

# Calling API
api_key ="AIzaSyB073-2rf6yfA2JBbJMLYFhUphKUJmQPe8"
youtube = build('youtube','v3',developerKey=api_key)


# Getting channel info
def get_channel_info(channel_id):
    channel_request = youtube.channels().list(
                    part = "snippet,contentDetails,Statistics",
                    id = channel_id)
                
    response1=channel_request.execute()
    for i in range(len(response1["items"])):
            data = dict(
                        Channel_Name = response1["items"][i]["snippet"]["title"],
                        Channel_Id = response1["items"][i]["id"],
                        Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                        Views = response1["items"][i]["statistics"]["viewCount"],
                        Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                        Channel_Description = response1["items"][i]["snippet"]["description"],
                        Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                        )
    return data

#Getting playlist ids
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

# Getting video_id 
def get_videoid_info(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
             
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

#getting video info

def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#get comment info
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information

#insert data into mongo db

client = pymongo.MongoClient('mongodb://localhost:27017')
mydb = client['Youtube_DB']

def data_into_mango(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_videoid_info(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = mydb["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    res=f"upload completed successfully for {channel_id}"
    return res

#table creation for channel
def channel_table(channel_name_s):
    mysqldb = sql.connect(host="localhost",
                    user="root",
                    password="Keerthi@12345",
                    database= "youtube_db",
                    )

    mycursor = mysqldb.cursor()

    try:
        create_query= '''create table if not exists channels(Channel_Name varchar(255),
                                                        Channel_Id varchar(255) Primary key,
                                                        Subscription_Count BIGINT,
                                                        Views BIGINT,
                                                        Total_Videos BIGINT,
                                                        Channel_Description TEXT ,
                                                        Playlist_Id varchar(255)
                                                        )'''
        mycursor.execute(create_query)
    except:
        print("Channel Table already created")

   
    single_channel_details= []
    mydb = client['Youtube_DB']
    coll1=mydb["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["channel_information"])

    df_single_channel= pd.DataFrame(single_channel_details)


    for index, row in df_single_channel.iterrows():
        insert_query = '''INSERT INTO channels (Channel_Name,
                                    Channel_Id,
                                    Subscription_Count,
                                    Views,
                                    Total_Videos,
                                    Channel_Description,
                                    Playlist_Id)
                                    values (%s, %s,%s, %s,%s, %s,%s)'''
        
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id']
                )

        try:
            mycursor.execute(insert_query,values)
            mysqldb.commit()
        except:
            news = f"Channel details of {channel_name_s} already exists"
            return news

#playlist table creation

def playlist_table(channel_name_s):
    mysqldb = sql.connect(host="localhost",
                        user="root",
                        password="Keerthi@12345",
                        database= "youtube_db",
                        )

    mycursor = mysqldb.cursor()

    create_query= '''create table if not exists playlists(PlaylistId varchar(100) Primary key,
                                                            Title varchar(100),
                                                            ChannelId varchar(100),
                                                            ChannelName varchar(100), 
                                                            PublishedAt timestamp,                              
                                                            VideoCount INT 
                                                            )'''
    

    mycursor.execute(create_query)
    mysqldb.commit()

    single_channel_details= []
    db=client["Youtube_DB"]
    coll1=mydb["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["playlist_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])


    for index, row in df_single_channel.iterrows():
            insert_query = '''INSERT INTO playlists (PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName, 
                                                    PublishedAt,                                              
                                                    VideoCount) 
                                                    values(%s,%s,%s,%s,%s,%s)'''
            publshed_date= datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            
            values=(row['PlaylistId'],
                    row['Title'],
                    row['ChannelId'],
                    row['ChannelName'],
                    publshed_date,
                    row['VideoCount']
                    )
            
            
            mycursor.execute(insert_query,values)
            mysqldb.commit()
    

#fun to covert duration into sec             
def duration_conv(duration_str):
    hours = 0
    minutes = 0
    seconds = 0

    if 'H' in duration_str:
        hours_index = duration_str.find('H')
        hours = int(duration_str[2:hours_index])

    if 'M' in duration_str:
        minutes_index = duration_str.find('M')
        if 'H' in duration_str:
            minutes = int(duration_str[hours_index + 1:minutes_index])
        else:
            minutes = int(duration_str[2:minutes_index])

    if 'S' in duration_str:
        seconds_index = duration_str.find('S')
        if 'M' in duration_str:
            seconds = int(duration_str[minutes_index + 1:seconds_index])
        elif 'H' in duration_str:
            seconds = int(duration_str[hours_index + 1:seconds_index])
        else:
            seconds = int(duration_str[2:seconds_index])
    total_seconds = hours * 3600 + minutes * 60 + seconds


    return total_seconds

#video table creation
def video_table(channel_name_s):
    mysqldb = sql.connect(host="localhost",
                            user="root",
                            password="Keerthi@12345",
                            database= "youtube_db",
                            )

    mycursor = mysqldb.cursor()

    create_query= '''create table if not exists videos(
                            Channel_Name varchar(255),
                            Channel_Id varchar(255),
                            Video_Id varchar(255) PRIMARY KEY,
                            Title varchar(255),
                            Tags text,
                            Thumbnail varchar(255),
                            Description text,
                            Published_Date datetime,
                            Duration int,
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Favorite_Count int,
                            Definition varchar(50),
                            Caption_Status varchar(50)                                                                              
                            )'''
    mycursor.execute(create_query)
    mysqldb.commit()
    

    single_channel_details= []
    coll1=mydb["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["video_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index, row in df_single_channel.iterrows():
                insert_query = '''INSERT INTO videos values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                published_date= datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                duration=duration_conv(row['Duration'])
                
                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        json.dumps(row['Tags']),
                        row['Thumbnail'],
                        row['Description'],
                        published_date,
                        duration,
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                    )
                
                
                mycursor.execute(insert_query,values)
                mysqldb.commit()
               

#table creation for comments
def comment_table(channel_name_s):
    mysqldb = sql.connect(host="localhost",
                        user="root",
                        password="Keerthi@12345",
                        database= "youtube_db",
                        )

    mycursor = mysqldb.cursor()

    create_query= '''create table if not exists comments(Comment_Id varchar(255) primary key,
                                                Video_Id varchar(255),
                                                Comment_Text text,
                                                Comment_Author varchar(255),
                                                Comment_Published datetime
                                                            )'''
    
    
    
    mycursor.execute(create_query)
    mysqldb.commit()
    
    single_channel_details= []
    coll1=mydb["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["comment_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index, row in df_single_channel.iterrows():
                insert_query = '''INSERT INTO comments (Comment_Id ,
                                                        Video_Id ,
                                                        Comment_Text ,
                                                        Comment_Author ,
                                                        Comment_Published) 
                                                        values(%s,%s,%s,%s,%s)'''
                published_date= datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                
                
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        published_date
                        )
                
                mycursor.execute(insert_query,values)
                mysqldb.commit()
            
def tables(channel_name):

    news= channel_table(channel_name)
    if news:
        return news
    else:  
        playlist_table(channel_name)
        video_table(channel_name)
        comment_table(channel_name)

        return "Tables successfully created"     

col1, col2 = st.columns(2)
with col1: 

    st.subheader(':violet[DATA COLLECTION ]')
    st.write(" To store the data from YouTube API to MongoDB data lake. Enter the Channel ID and Click the below **Collect and store** Button")
    channel_id= st.text_input("Enter the channnel id:")
    channels= [ch.strip() for  ch in channel_id.split(',')]
    

    if st.button("Collect and store"):
        for channel in channels:
            ch_ids=[]
            db = client["Youtube_DB"]
            coll1 = mydb["channel_details"]
            for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                    ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                insert=data_into_mango(channel)
                st.success(insert)
    st.subheader(':violet[DATA MIGRATION ]')
    st.write(" To Migrate the data to MYSQL. Click the below **Migrate to sql** Button")

    all_channels= []
    coll1=mydb["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
    unique_channel= st.selectbox("Select the Channel",all_channels)

    if st.button("Migrate to sql"):
        Table=tables(unique_channel)
        st.success(Table)
with col2:
    
    st.header(':violet[ Data Analysis ]')
    st.write (''' **Analysis of a collection of channel data** depends on your question selection and gives the output in a table format.''')

    #SQL connection
    mysqldb = sql.connect(host="localhost",
                            user="root",
                            password="Keerthi@12345",
                            database= "youtube_db",
                            )

    mycursor = mysqldb.cursor()
        
    question = st.selectbox(
        'Please Select Your Question',
        ('1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))
    
    if question == '1. What are the names of all the videos and their corresponding channels?':
        query1 = "select Title , Channel_Name from videos;"
        mycursor.execute(query1)
        t1=mycursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

    elif question =='2. Which channels have the most number of videos, and how many videos do they have?':
        query2 = "select Channel_Name ,Total_Videos from channels order by Total_Videos desc;"
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","Total Videos"]))

    elif question =='3. What are the top 10 most viewed videos and their respective channels?':
        query3 = "select Title, Channel_Name,views from videos where Views is not null order by Views desc limit 10;"
        mycursor.execute(query3)
        t3=mycursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Video Title","Channel Name","Total Views"]))

    elif question =='4. How many comments were made on each video, and what are their corresponding video names?':
        query4 = "select Title, comments from videos ;"
        mycursor.execute(query4)
        t4=mycursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Title","No of comments"]))

    elif question =='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = "select Title,Channel_Name, Likes from videos order by Likes desc;"
        mycursor.execute(query5)
        t5=mycursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Video Title","Channel Name","No of Likes"]))

    elif question =='6. What is the total number of likes for each video, and what are their corresponding video names?':     
        query6 = "select Title, Likes from videos ;"
        mycursor.execute(query6)
        t6=mycursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Video Title","No of Likes"]))

    elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?': 
      query7 = "select Channel_Name, Views from channels ;"
      mycursor.execute(query7)
      t7=mycursor.fetchall()
      st.write(pd.DataFrame(t7, columns=["Channel Name","Total Views"])) 

    elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
        query8 = "select Distinct(Channel_Name) from videos where published_date LIKE '2022%' ;"
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Channel Name"]))

    elif question ==  '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9 = "select Channel_Name, round(avg(duration),2)  from videos group by Channel_Name;"
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel Name","Average Duration"]))

    elif question ==  '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10 = "select title,Channel_Name, Comments  from videos order by Comments desc;"
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Video Name","Channel Name","Highest Comments"]))


         


