import tweepy
import os
import json
import requests
import pandas as pd
from datetime import datetime

def run_etl():
    consumer_key = "" 
    consumer_secret = "" 
    access_token = ""    
    access_token_secret = "" 
    bearer_token = ''

    client = tweepy.Client(bearer_token=bearer_token, consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token, access_token_secret=access_token_secret, return_type = requests.Response)
    print ("A")
    queryTopics ='#ChatGPT ("university" OR "uni" OR "examination" OR "exam" OR "exams" OR "assignment" OR "test" OR "tests") lang:en'
    # Save tweets to a JSON file
    # e.g. earthquake.json
    # tweets = client.search_recent_tweets(query=queryTopics, tweet_fields=["public_metrics", "created_at", "context_annotations",], max_results=100)
    # print ("B")

    # change limit to 2 for testing
    # 2 means 2 pages of 100 tweets each
    tweets = tweepy.Paginator(client.search_recent_tweets, query=queryTopics, tweet_fields=["public_metrics", 'context_annotations', 'created_at'], max_results=100, limit = 2)
    # print ("C")

    tweets_data = []
    for page in tweets:
        for tweet in page.json()['data']:
            tweets_data.append(tweet)
        # print (page)

    print (len(tweets_data))   
    tweets_df = pd.json_normalize(tweets_data)
    tweets_df.to_json("tweets.json", orient="records")


run_etl()