import sys
import string
import sys
import warnings
from datetime import datetime

# Import the libraries
import numpy as np
import pandas as pd
import pytz
from langdetect import detect
# Data Preprocessing and Feature Engineering
from textblob import TextBlob
from youtube_easy_api.easy_wrapper import *

from workers.sensors.sensory_memory import Sensor

utc = pytz.UTC

warnings.simplefilter(action='ignore', category=FutureWarning)
sys.setrecursionlimit(1500)


class YoutubeSensor(Sensor):
    def __init__(self):
        super().__init__(name="YOUTUBE", internal_id="YOUTUBE", interval=86400, attention_limit=1000, source="YOUTUBE")
        # start
        self.YouTubeApiKey = 'AIzaSyAiq-B66p7wtESH6m0FBnJzjg0hFpNu9JE'
        self.Youtube = build('youtube', 'v3', developerKey=self.YouTubeApiKey)
        # end

    def convertDate(self, d):
        date = datetime.fromisoformat(d[:-1])
        return date.strftime('%Y-%m-%d %H:%M:%S')

    def localize_date(self, date):
        return utc.localize(date)

    def Start(self, instances: list):
        for record in instances:
            df = self.FetchSensorData(record['keyword'])

            if df.shape[0] == 0:
                continue
            else:
                temp = np.datetime64(record['scrapped_at'])
                latest = df[df['published_date'] > temp]
                senti_df = self.DoLowLevelPerception(latest)
                self.AddMemory(senti_df)

    def FetchSensorData(self, keyword) -> pd.DataFrame:
        # start code
        req = self.Youtube.search().list(q=keyword, part='snippet', type='video', maxResults=500)
        res = req.execute()

        title = []
        video_id = []
        published_date = []
        description = []
        views = []
        liked = []
        disliked = []
        liked = []
        comment_count = []

        for i in range(len(res) - 1):
            title.append(res['items'][i]['snippet']['title'])
            video_id.append(res['items'][i]['id']['videoId'])
            date = datetime.fromisoformat(res['items'][i]['snippet']['publishedAt'][:-1])
            published_date.append(date.strftime('%Y-%m-%d %H:%M:%S'))
            description.append(res['items'][i]['snippet']['description'])
            stats = self.Youtube.videos().list(id=res['items'][i]['id']['videoId'], part='statistics').execute()
            if len(stats['items'][0]['statistics']) == 5:
                views.append(stats['items'][0]['statistics']['viewCount'])
                liked.append(stats['items'][0]['statistics']['likeCount'])
                disliked.append(stats['items'][0]['statistics']['dislikeCount'])
                comment_count.append(stats['items'][0]['statistics']['commentCount'])
            else:
                views.append('NA')
                liked.append('NA')
                disliked.append('NA')
                comment_count.append('NA')

        data = {'title': title, 'video_id': video_id, 'published_date': published_date, 'description': description,
                'views': views, 'liked': liked, 'disliked': disliked, 'comment_count': comment_count}

        df = pd.DataFrame(data)
        if not df.empty:
            df["keyword"] = str(keyword).lower()
            df['published_date'] = pd.to_datetime(df['published_date'])
            df['published_date'].apply(self.localize_date)

        return df

    def AddMemory(self, memory_content: pd.DataFrame, source="YOUTUBE"):
        super().AddMemory(memory_content, source)

    def RemoveMemory(self, content_cue_to_remove):
        pass

    def dataCleaning(self, text):
        from nltk.corpus import stopwords
        punctuation = string.punctuation
        stopwords = stopwords.words('english')
        text = text.lower()
        text = "".join(x for x in text if x not in punctuation)
        words = text.split()
        words = [w for w in words if w not in stopwords]
        text = " ".join(words)

        return text

    def detect_language(self, text):
        try:
            lang = detect(text)
        except:
            try:
                lang = TextBlob(text).detect_language()
            except:
                lang = "None"
        return lang

    def DoLowLevelPerception(self, posts: pd.DataFrame) -> pd.DataFrame:
        if posts.shape[0] == 0:
            return posts

        posts['cleaned_description'] = posts['description'].apply(self.dataCleaning)

        posts['polarity'] = posts['cleaned_description'].apply(lambda tweet: TextBlob(tweet).sentiment.polarity)
        posts['subjectivity'] = posts['cleaned_description'].apply(lambda tweet: TextBlob(tweet).sentiment.subjectivity)
        posts['language'] = posts['cleaned_description'].apply(self.detect_language)

        del posts['cleaned_description']

        return posts


youtube_sensor = YoutubeSensor()

if __name__ == '__main__':
    # ListOfBrands = [ #'Amazon', 'Apple', 'Google', 'Microsoft', 'Tencent', 'Facebook', 'Alibaba', 'Visa',
    # 'McDonalds', 'MasterCard', 'Moutai', 'Nvidia', 'Verizon', 'AT&T', 'IBM', 'Coca-Cola', 'Nike', 'Instagram',
    # 'PayPal', 'Adobe', 'Louis Vuitton', 'UPS', 'Intel', 'Netflix', 'SAP', 'Accenture', 'Oracle', 'Starbucks',
    # 'Walmart', 'Xfinity', 'Marlboro', 'Disney', 'Meituan', 'Texas Instruments', 'Salesforce', 'Qualcomm',
    # 'Spectrum', 'Youtube', 'Chanel', 'Cisco', 'Samsung', 'Hermes Paris', 'JD.com', 'Tiktok', 'T-Deutseche Telekom',
    # 'Tesla', "L'oreal", 'Ping An', 'Huawei', 'Industrial and Commmercial Bank of China', 'Zoom', 'Intuit',
    # 'LinkedIn', 'Costco', 'Gucci', 'Advance Micro Device', 'Tata Consultancy services', 'Xbox', 'Vodafone',
    # 'American Express', 'Wells frago', 'RBC', 'Toyota', 'Haier', 'HDFC Bank', 'Mercedes-Benz', 'China Mobile
    # Limited', 'Budweiser', 'Xiaomi Technologies', 'BMW', 'Dell Technologies', 'Life Insurace Corporation of India',
    # 'J.P. Morgan', 'Siemens', 'FedEx', 'Baidu', 'Uber', 'Adidas', 'Chase', 'Pinduoduo', 'Zara', 'Ikea',
    # 'UnitedHealthCare', 'Lowes', 'AIA Group', 'NTT Group', 'Autodesk', 'TD Bank Group', 'Orange', 'DHL',
    # 'Didi chuxing', 'China Construction Bank', 'Pampers', 'KE.com', 'Commonwealth Bank of Australia',
    # 'Bank of Amercia', 'Spotify', 'Colgate'] for keyword in ListOfBrands: if youtube_sensor.AttendTo(keyword):
    # print("data scrapped successful against ", keyword) else: print("data scrapped failed against ", keyword)
    pass
