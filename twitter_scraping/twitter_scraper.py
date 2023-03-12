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
    queryTopics ='#ChatGPT ("university" OR "uni" OR "examination" OR "exam" OR "exams" OR "assignment" OR "test" OR "tests")'
    # Save tweets to a JSON file
    # e.g. earthquake.json
    tweets = client.search_recent_tweets(query=queryTopics, tweet_fields=["public_metrics", "created_at", "context_annotations",], max_results=100)
    print ("B")

    # tweets = tweepy.Paginator(client.search_recent_tweets, query=queryTopics, tweet_fields=['context_annotations', 'created_at'], max_results=100).flatten(limit=200)
    print ("C")
    print (tweets)

    tweets_dict = tweets.json() 

    # Extract "data" value from dictionary
    tweets_data = tweets_dict['data'] 

    # Transform to pandas Dataframe
    tweets_df = pd.json_normalize(tweets_data) 
    print ("D")
    print (tweets_df.head(10))


    # # output json file
    tweets_df.to_json("twitter_scraped.json", orient="records")

    # Please verify that your Python script works by running it stand-alone (not as part of DAG)
    # You can use Jupyter Notebook for this.
    
run_etl()