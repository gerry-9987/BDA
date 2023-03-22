import json
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_pushshift_data(after, before, sub):
    url = 'https://api.pushshift.io/reddit/search/submission/?&after='+str(after)+'&before='+str(before)+'&subreddit='+str(sub)+'&size=1000'
    print(url)
    req = requests.get(url)
    data = json.loads(req.text, strict=False)
    return data['data']

def get_submission_data(submission, submission_list):
    id = submission['id']
    title = submission['title']
    body = submission['selftext']
    author = submission['author']
    created = submission['utc_datetime_str']
    upvotes = submission['score']
    num_of_comments = submission['num_comments']
    link = submission['permalink']
    subreddit = submission['subreddit']
    submission_list.append({'id': id, 'title': title, 'body': body, 'author': author, 'created': created, 'upvotes': upvotes, 'num_of_comments': num_of_comments, 'link': link, 'subreddit': subreddit})
    return submission_list

def get_subreddit_list():
    with open('./subreddit.txt', 'r') as subreddit_file:
        subreddit = subreddit_file.read()
        subreddit_list = subreddit.split("\n")
        return subreddit_list
    
def etl():
    subreddit_list = get_subreddit_list()
    for sub in subreddit_list:
        before = int(datetime.now().timestamp())
        after = int((datetime.now() - relativedelta(months=1)).timestamp())
        submission_list = []
        count = 0
        while before > after:
            pushshift_data = get_pushshift_data(after, before, sub)

            for submission in pushshift_data:
                submission_list = get_submission_data(submission, submission_list)
            
            if pushshift_data[-1]['retrieved_utc'] == before:
                break

            before = pushshift_data[-1]['retrieved_utc']

            with open('./data/'+str(sub)+str(count)+'.json', 'w') as json_file:
                json_object = json.dumps(pushshift_data, indent=4)
                json_file.write(json_object)
            
            count+=1
        
        with open('./data/reddit_'+str(sub)+'.json', 'w') as json_file:
            json_object = json.dumps(submission_list, indent=4)
            json_file.write(json_object)

etl()