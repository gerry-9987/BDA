import openai
import pandas as pd
import json

def make_api_call(df, batch_size):
    # Define the function to call OpenAI's chatcomplete endpoint
    def call_openai(model, prompt):
        print('=====calling a batch=====')
        response = openai.ChatCompletion.create(model=model, messages=[{'role':'user', 'content':prompt}])
        print('Response choices:', response['choices'][0]['message']['content'])
        print('Response end reason:', response['choices'][0]['finish_reason'])
        print('====batch is done=====')

        return response['choices'][0]['message']['content']

    # Define the function to score sentiment of a batch of tweets
    def score_batch(batch):
        text = '\n'.join([f'id: {tweet[1]} \n tweet: {tweet[2]}\n' for tweet in batch])
        response = call_openai('gpt-3.5-turbo', f'There are 6 different moods we want to score: happy, calm, alert, kind, sure, vital. Please score the sentiment of the following tweets on a scale of -1 to 1, where -1 is very negative, 0 is neutral, and 1 is very positive. A -1.00 on happy means extremely unhappy, while 1.00 on happy means extremely happy. Give your response in json format for all the tweets below, e.g. [{\'id\':tweet_id, \'score_happy\':1.00, score_calm:0.35, \'score_alert\':0.52, \'score_sure\':-0.92, \'score_kind\':-0.45, \'score_vital\':-0.23}, {\'id\':tweet_id_2, score_happy...}]\n{text}\n')
        return response
    
    # Extract the required columns as a list of tuples
    csv_raw = list(df.itertuples(index=False, name=None))

    # Process the data in batches
    responses = []
    for i in range(0, len(csv_raw), batch_size):
        batch = csv_raw[i:i+batch_size]
        resp = score_batch(batch)
        responses.append(resp)

    # Return the results as a list of dictionaries
    return responses