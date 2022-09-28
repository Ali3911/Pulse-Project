import warnings
# Import the libraries
from datetime import datetime

import numpy as np
import pandas as pd

from workers.sensors.sensory_memory import Sensor

warnings.simplefilter(action='ignore', category=FutureWarning)

# Data Preprocessing and Feature Engineering
from textblob import TextBlob
import string

# twint libraries
import twint
import pytz

utc = pytz.UTC


class TwitterSensor(Sensor):
    def __init__(self):
        super().__init__(name="TWITTER", internal_id="TWITTER", interval=86400, attention_limit=1000, source="TWITTER")

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
                latest = df[df['date'] > temp]
                senti_df = self.DoLowLevelPerception(latest)
                self.AddMemory(senti_df)

    def FetchSensorData(self, keyword):
        c = twint.Config()
        c.Search = keyword
        c.Limit = 500
        c.Pandas = True
        twint.run.Search(c)
        df = twint.storage.panda.Tweets_df
        df = df.loc[:, df.columns.intersection(
            ['user_id', 'username', 'name', 'date', 'tweet', 'nlikes', 'hashtags', 'link', 'language'])]
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df["keyword"] = str(keyword).lower()
            df['date'].apply(self.localize_date)

        return df

    def AddMemory(self, memory_content: pd.DataFrame, source="TWITTER"):
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

    def DoLowLevelPerception(self, posts: pd.DataFrame) -> pd.DataFrame:
        if posts.shape[0] == 0:
            return posts

        posts['cleaned_tweet'] = posts['tweet'].apply(self.dataCleaning)

        posts['polarity'] = posts['cleaned_tweet'].apply(lambda tweet: TextBlob(tweet).sentiment.polarity)
        posts['subjectivity'] = posts['cleaned_tweet'].apply(lambda tweet: TextBlob(tweet).sentiment.subjectivity)
        del posts['cleaned_tweet']

        return posts


twitter_sensor = TwitterSensor()

