import pandas as pd
import os
import datetime


#for json object (not file) that is scraped from twitter 

twitter_df = pd.read_json("") #read the json object string
twitter_df = twitter_df.drop_duplicates(subset=["text"])
twitter_df = twitter_df.dropna(subset=["text"])
twitter_df = twitter_df.drop(columns=['id', 'created_at','edit_history_tweet_ids','context_annotations', 'public_metrics.retweet_count',	'public_metrics.reply_count',	'public_metrics.like_count',	'public_metrics.quote_count',	'public_metrics.impression_count'])
#transfer to csv - json will be have the datetime of the scrape as the name "datetime_cleaned"
twitter_df.to_csv("") #to the read folder in s3 bucket - naming convention ("tweets_datetime")


#for json object (not file) that is scraped from reddit 

reddit_df = pd.read_json("") #read the json object string
reddit_df = reddit_df.drop_duplicates(subset=["title"])
reddit_df = reddit_df.dropna(subset=["title"])
reddit_df = reddit_df.drop(columns=['id', 'author','created','upvotes','num_of_comments','link','subreddit'])
#transfer to csv - json will be have the datetime of the scrape as the name "datetime_cleaned"
reddit_df.to_csv("") #to the read folder in s3 bucket - naming convention ("reddit_subreddit_datetime")