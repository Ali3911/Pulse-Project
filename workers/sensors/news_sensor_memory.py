import string
from datetime import datetime
import numpy as np
import pandas as pd
import pytz
from langdetect import detect
from newsapi.newsapi_client import NewsApiClient
from textblob import TextBlob

from workers.sensors.sensory_memory import Sensor

utc = pytz.UTC


class NewsSensor(Sensor):
    def __init__(self):
        super().__init__(name="NEWS", internal_id="NEWS", interval=86400, attention_limit=1000, source="NEWS")
        # Initiating the news api client with key
        self.newsapi = NewsApiClient(api_key='8f2a9534ae26466f8ae4d0f12643f94c')

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
                latest = df[df['publishedAt'] > temp]
                senti_df = self.DoLowLevelPerception(latest)
                self.AddMemory(senti_df)

    def FetchSensorData(self, keyword) -> pd.DataFrame:
        all_articles = self.newsapi.get_everything(q=keyword)

        articles = all_articles['articles']
        news_data = pd.DataFrame(articles)

        count = 0
        for post in all_articles['articles']:
            news_data['source'][count] = post['source']['name']
            count = count + 1

        news_data["keyword"] = str(keyword).lower()

        if 'publishedAt' in list(news_data.columns):
            news_data['publishedAt'] = news_data['publishedAt'].apply(self.convertDate)

        if not news_data.empty:
            news_data['publishedAt'] = pd.to_datetime(news_data['publishedAt'])
            news_data['publishedAt'].apply(self.localize_date)

        return news_data

    def live_data_response(self, keyword, source) -> bool:
        return super().live_data_response(keyword, source)

    def AddMemory(self, memory_content: pd.DataFrame, source="NEWS"):
        super().AddMemory(memory_content, source=source)

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

    def DoLowLevelPerception(self, posts: pd.DataFrame):
        if posts.shape[0] == 0:
            return posts
        posts["combined"] = (posts["title"] + posts["description"] + posts["content"])
        posts = posts.dropna(axis=0, subset=['combined'])
        posts['combined'].astype('str')
        posts['cleaned'] = posts['combined'].apply(self.dataCleaning)

        posts['polarity'] = posts['cleaned'].apply(lambda tweet: TextBlob(tweet).sentiment.polarity)
        posts['subjectivity'] = posts['cleaned'].apply(lambda tweet: TextBlob(tweet).sentiment.subjectivity)
        posts['language'] = posts['cleaned'].apply(self.detect_language)
        del posts['combined']

        return posts


news_sensor = NewsSensor()

if __name__ == '__main__':
    pass
