import json 
import pandas as pd 
import time
import sqlalchemy
import ctypes
import pymysql
from InstagramAPI import InstagramAPI
class instagramAnalytics():
    def extract(self,usrnm, pswrd):
        global InstagramAPI 
        InstagramAPI= InstagramAPI(usrnm,pswrd)
        InstagramAPI.login()
        print("Instagram Login Successful")
        instagramAnalytics.allPosts(self,InstagramAPI)
        #collect post data
        instagramAnalytics.allComments(self,InstagramAPI)
        #collect Comments data
        instagramAnalytics.allLikes(self,InstagramAPI)
        #collect likes data
        instagramAnalytics.allFollow(self,InstagramAPI)
        # collect followers and following data
        InstagramAPI.logout()
        ctypes.windll.user32.MessageBoxW(0, "Data Extract Completed", "Success")
        # alert window after extraction is finished
        instagramAnalytics.loadDataToSQL(self)
        #load data to SQL
    def allPosts(self,InstagramAPI):
        engine = sqlalchemy.create_engine("mysql+pymysql://root:R00t@admin@localhost:3306/socialmediadb")
        #this will change based on the database that we need to connect
        print("Connected to Social Media Database")
        print("Extracting Posts")
        myposts = []
        has_more_posts = True
        max_id = ""
        while has_more_posts :
            InstagramAPI.getSelfUserFeed(maxid=max_id)
            if InstagramAPI.LastJson['more_available'] is not True:
                has_more_posts = False # all posts are extracted. Loop stop
                #print("Loop Finished")
            max_id = InstagramAPI.LastJson.get('next_max_id','')
            myposts.extend(InstagramAPI.LastJson['items']) # merge list
            time.sleep(2) ## Slow down execution
        #print(myposts)
        #print(len(myposts))--test purpose
        postDF = pd.DataFrame(myposts)
        imageDF = postDF[['image_versions2','pk']]
        imageDF.to_json('ImageURL.json',orient="records")
        #store imdia url in json so that we can use this json to store url in database.
        #if we want to store vedio stats
        #filterVideoPost = postDF[postDF['media_type'] == 2]
        #videoDF = filterVideoPost[['video_versions','video_duration','view_count','pk']]
        #videoDF.to_csv('VideoURL.csv',sep=',',encoding='utf-8')
        SQLpostDF = postDF[['can_view_more_preview_comments','can_viewer_reshare','can_viewer_save','client_cache_key','code','comment_count'
        ,'comment_likes_enabled','comment_threading_enabled','device_timestamp','filter_type','has_liked'
        ,'has_more_comments','id','lat','like_count','lng','media_type','photo_of_you','pk','taken_at']].copy()
        #print(SQLpostDF)
        SQLpostDF.to_sql(name='allpostinstagram',con=engine, if_exists='replace',index=False,schema='socialmediadb')
    def allComments(self,InstagramAPI):
        engine = sqlalchemy.create_engine("mysql+pymysql://root:R00t@admin@localhost:3306/socialmediadb")
        mediaIDcomment_DF = pd.read_sql("SELECT pk as media_id FROM socialmediadb.allpostinstagram", con = engine)
        # extract media ID stored by all post function to get all comments for each post
        print("Extracting Comments")
        commentData=[]
        mediaID_append =[]
        for i in range(len(mediaIDcomment_DF)):
            InstagramAPI.getMediaComments(str(mediaIDcomment_DF.get_value(index=i,col='media_id')))
            commentData.extend(InstagramAPI.LastJson['comments'])
            for loop  in range(len(InstagramAPI.LastJson['comments'])) :
                mediaID_append.append(str(mediaIDcomment_DF.get_value(index=i,col='media_id')))
            time.sleep(2)
        #print(commentData) --test purpose
        commentsDF = pd.DataFrame(commentData)
        commentsDF['media_id'] = mediaID_append
        commentUserDF = commentsDF[['user','pk']] #can be used while storing user comments for sentiment analysis in the future
        commentUserDF.to_json('CommentUser.json',orient="records")
        SQLCommentsDF = commentsDF[['media_id','pk', 'text', 'type', 
        'created_at', 'created_at_utc', 'content_type', 'status', 'bit_flags' , 'did_report_as_spam', 
        'inline_composer_display_condition']].copy()
        SQLCommentsDF.to_sql(name='allcommentsinstagram',con=engine, if_exists='replace',index=False,schema='socialmediadb')
    def allLikes(self,InstagramAPI):
        engine = sqlalchemy.create_engine("mysql+pymysql://root:R00t@admin@localhost:3306/socialmediadb")
        mediaID_DF = pd.read_sql("SELECT pk as media_id FROM socialmediadb.allpostinstagram", con = engine)
        # extract media ID stored by all post function to get all likes for each post
        likesData=[]
        mediaID_append =[]
        print("Extracting Likes")
        for i in range(len(mediaID_DF)):
            InstagramAPI.getMediaLikers(str(mediaID_DF.get_value(index=i,col='media_id')))
            likesData.extend(InstagramAPI.LastJson['users'])
            for loop  in range(len(InstagramAPI.LastJson['users'])) :
                mediaID_append.append(str(mediaID_DF.get_value(index=i,col='media_id')))
            time.sleep(2) # slow down the loop
        #print(commentData)
        #print(mediaID_append)
        likesDF = pd.DataFrame(likesData)
        #print(commentsDF)
        likesDF['media_id'] = mediaID_append
        #print(commentsDF)
        SQLlikesDF = likesDF[['media_id','pk', 'username', 'full_name']].copy()
        SQLlikesDF.to_sql(name='alllikesinstagram',con=engine, if_exists='replace',index=False,schema='socialmediadb')
    def allFollow(self,InstagramAPI):
        engine = sqlalchemy.create_engine("mysql+pymysql://root:R00t@admin@localhost:3306/socialmediadb")
        InstagramAPI.login()
        print("Extracting Followers")
        InstagramAPI.getProfileData()
        user_id = InstagramAPI.LastJson['user']['pk']
        following = []
        next_max_id = True
        while next_max_id:
            if next_max_id == True:
                next_max_id = ''
            _ = InstagramAPI.getUserFollowings(user_id,maxid = next_max_id)
            following.extend(InstagramAPI.LastJson.get('users',[]))
            next_max_id = InstagramAPI.LastJson.get('next_max_id','')
            time.sleep(1)
        following_users_list = following
        #following user data loaded
        followers = []
        next_max_id = True
        while next_max_id:
            if next_max_id == True:
                next_max_id = ''
            _ = InstagramAPI.getUserFollowers(user_id,maxid = next_max_id)
            followers.extend(InstagramAPI.LastJson.get('users',[]))
            next_max_id = InstagramAPI.LastJson.get('next_max_id','')
            time.sleep(1)
        followers_users_list = followers
        #Following user data loaded
        FollowingDF = pd.DataFrame(following_users_list)
        FollowingDFSQL = FollowingDF[['username','pk']].copy()
        #print("Following")
        #print(FollowingDF)
        #InstagramAPI.getUserFollowers(user_id)
        #print(len(InstagramAPI.LastJson['users']))
        FollowersDF = pd.DataFrame(followers_users_list)
        FollowersDFSQL = FollowersDF[['username','pk']].copy()
        #print("Followers")
        #print(FollowersDF)
        FollowingDFSQL.to_sql(name='followinglist',con=engine, if_exists='replace',index=False,schema='socialmediadb')
        FollowersDFSQL.to_sql(name='followerslist',con=engine, if_exists='replace',index=False,schema='socialmediadb')
    def loadDataToSQL(self):
        # connect to MySQL
        con = pymysql.connect(host = 'localhost',user = 'root',passwd = 'R00t@admin',db = 'socialmediadb')
        cursor = con.cursor()
        count_DF = pd.read_sql("select pk from allcommentsinstagram",con=con)
        count_post_DF = pd.read_sql("select pk from allpostinstagram",con=con)
        cursor.execute("TRUNCATE TABLE commentuser")
        with open(r'CommentUser.json') as data_file:
            jsondata = json.load(data_file)
            for key in range(len(count_DF)):
                nested_json = jsondata[key]
                pk = nested_json['pk']
                usercomment = nested_json['user']
                username = usercomment['username']
                fullname = usercomment['full_name']
                inputVariable =(str(pk) + ";" +username +";" + fullname)
                cursor.execute("insert into commentuser (value) values (%s)",inputVariable)
        cursor.execute("TRUNCATE TABLE allpostmediaurl")
        with open(r'ImageURL.json') as data_file:
            jsondata = json.load(data_file)
            for key in range(len(count_post_DF)):
                nested_json = jsondata[key]
                image_version = nested_json["image_versions2"]
                pk = nested_json["pk"]
                if not (image_version is None):
                    candidate = image_version["candidates"]
                    if not (candidate is None):
                        value_url = candidate[1]["url"]
                        #print(value_url)
                        #print(value_pk)                
                        insertedValue = value_url+";"+str(pk);
                        cursor.execute("INSERT INTO allpostmediaurl (url) VALUES (%s)", (insertedValue))
        cursor.execute("CALL `socialmediadb`.`loadInstagramPost`();")
        cursor.execute("CALL `socialmediadb`.`loadInstagramComments`();")
        cursor.execute("CALL `socialmediadb`.`loadInstagramLikes`();")
        con.commit()
        con.close()

        
        

