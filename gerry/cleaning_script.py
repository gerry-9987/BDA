import pandas as pd
import numpy as np
import re

import html
from unicodedata import normalize

import string

import nltk
from nltk.stem import WordNetLemmatizer

import sys
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.parse.malt import MaltParser
from nltk.corpus import words
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')
nltk.download('stopwords')

#if the nltk downloads don't work
# import nltk
# import ssl

# try:
#     _create_unverified_https_context = ssl._create_unverified_context
# except AttributeError:
#     pass
# else:
#     ssl._create_default_https_context = _create_unverified_https_context

# nltk.download()

#Functions to clean text
"""
Code references:
    https://pythonguides.com/remove-unicode-characters-in-python/
    https://www.kite.com/python/answers/how-to-decode-html-entities-in-python
"""
def decode_text(text):
    # remove non-ASCII characters in string
    decoded_text = text.encode('ascii', 'ignore').decode('utf-8')

    # decode HTML entities
    decoded_html = html.unescape(decoded_text)
    return ''.join([word for word in decoded_html if word.isprintable()])

"""
Code reference:
    https://stackoverflow.com/questions/11331982/how-to-remove-any-url-within-a-string-in-python
"""
def remove_links(text):
    return re.sub(r"http\S+", "", text)

"""
Code reference:
    https://catriscode.com/2021/03/02/extracting-or-removing-mentions-and-hashtags-in-tweets-using-python/
"""
def remove_mentions(text):
    return re.sub("@[A-Za-z0-9_]+","", text)

def remove_hashtags(text):
    return re.sub("#[A-Za-z0-9_]+","", text)

def remove_stopwords(words_list):
    stop_list = stopwords.words("english")
    stop_list.append("filler")
    return [word for word in words_list if word not in stop_list]

def pos_to_wordnet(nltk_tag):
    if nltk_tag.startswith('J'):
        return wordnet.ADJ
    elif nltk_tag.startswith('V'):
        return wordnet.VERB
    elif nltk_tag.startswith('N'):
        return wordnet.NOUN
    elif nltk_tag.startswith('R'):
        return wordnet.ADV
    else:
        return None

def lemmatize_words(word_list):
    lemmatizer = nltk.stem.WordNetLemmatizer()
    # POS (part-of-speech) tagging
    # nltk_tagged -> a list of tuples (word, pos tag)
    nltk_tagged = nltk.pos_tag(word_list)

    # returns a list of tuples of words and their wordnet_tag (after conversion from NLTK tag)
    wordnet_tagged = list(map(lambda x: (x[0], pos_to_wordnet(x[1])), nltk_tagged))

    # lemmatizing
    lemmatized_words = []
    for word, tag in wordnet_tagged:
        if tag is not None:
            # need POS tag as 2nd argument as it helps lemmatize the words more accurately
            lemmatized_words.append(lemmatizer.lemmatize(word, tag))
        elif tag in [wordnet.NOUN]:
            lemmatized_words.append(lemmatizer.lemmatize(word))
    return lemmatized_words

def clean_original_text(text):
    text = str(text)
    text = text.lower()
    clean_list = []
    sentence_list = nltk.sent_tokenize(text)
    for sentence in sentence_list:
        decoded_sentence = decode_text(sentence)
        linkless_sentence = remove_links(decoded_sentence)
        mentionless_sentence = remove_mentions(linkless_sentence)
        tagless_sentence = remove_hashtags(mentionless_sentence)
        words_list = nltk.RegexpTokenizer(r'\w+').tokenize(tagless_sentence)
        lemmatized_words = lemmatize_words(words_list)
        useful_words = remove_stopwords(lemmatized_words)

        if len(useful_words) > 0:
            clean_list.extend(useful_words)
    clean_text = ' '.join(clean_list)

    return clean_text

def id_to_str(text):
    text = str(text)

    return text

#for json object (not file) that is scraped from twitter 

twitter_df = pd.read_json("") #read the json object string
twitter_df["clean_text"] = twitter_df["text"].apply(clean_original_text)
twitter_df["id"] = twitter_df["id"].apply(id_to_str)
twitter_df = twitter_df.drop_duplicates(subset=["clean_text"])
twitter_df = twitter_df.dropna(subset=["clean_text"])
twitter_df = twitter_df.drop(columns=['created_at','edit_history_tweet_ids','context_annotations', 'public_metrics.retweet_count',	'public_metrics.reply_count',	'public_metrics.like_count',	'public_metrics.quote_count',	'public_metrics.impression_count'])
twitter_df["text"] = twitter_df["text"].str.lower()
twitter_df = twitter_df.replace('\n',' ', regex=True)
twitter_df = twitter_df.apply(lambda x: x.str.replace(',',' '))
twitter_df = twitter_df.set_index("id")
twitter_df = twitter_df.reset_index()
# twitter_df = twitter_df.reset_index(drop=True)
# twitter_df.index.names = ['id']
# twitter_df = twitter_df.reset_index()
#transfer to csv - json will be have the datetime of the scrape as the name "datetime_cleaned"
twitter_df.to_csv("", index=False) #to the read folder in s3 bucket - naming convention ("tweets_datetime")


#for json object (not file) that is scraped from reddit 

reddit_df = pd.read_json("") #read the json object string
reddit_df["clean_title"] = reddit_df["title"].apply(clean_original_text)
reddit_df["clean_body"] = reddit_df["body"].apply(clean_original_text)
reddit_df["id"] = reddit_df["id"].apply(id_to_str)
reddit_df = reddit_df.drop_duplicates(subset=["clean_title"])
reddit_df = reddit_df.dropna(subset=["clean_title"])
reddit_df = reddit_df.drop(columns=['author','created','upvotes','num_of_comments','link','subreddit'])
reddit_df["title"] = reddit_df["title"].str.lower()
reddit_df["body"] = reddit_df["body"].str.lower()
reddit_df = reddit_df.replace('\n',' ', regex=True)
reddit_df = reddit_df.apply(lambda x: x.str.replace(',',' '))
reddit_df = reddit_df.set_index("id")
reddit_df = reddit_df.reset_index()
#transfer to csv - json will be have the datetime of the scrape as the name "datetime_cleaned"
reddit_df.to_csv("", index=False) #to the read folder in s3 bucket - naming convention ("reddit_subreddit_datetime")