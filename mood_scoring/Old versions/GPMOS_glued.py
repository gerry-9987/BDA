# Set variables
database = "my_database"
table = "my_table"
new_column = "sentiment_scores"
bucket_name = "my_bucket"
file_name = "raw_data.csv"
batch_size = 50
api_key = "my_openai_api_key"

import boto3
import pandas as pd
import requests
import json
import io
import csv
import openai

# Set variables
database = "my_database"
table = "my_table"
new_column = "sentiment_scores"
bucket_name = "my_bucket"
file_name = "raw_data.csv"
batch_size = 50
api_key = "my_openai_api_key"

# Function to add a new column to an Athena table
def add_column_to_table(database, table, column_name, data_type):
    query = f"ALTER TABLE {database}.{table} ADD COLUMNS ({column_name} {data_type})"
    execute_athena_query(query)

# Function to create an Athena table if it does not exist
def create_table_if_not_exists(database, table, df):
    query = f"CREATE TABLE IF NOT EXISTS {database}.{table} (id string, text_content string)"
    execute_athena_query(query)

# Function to execute an Athena query
def execute_athena_query(query):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{bucket_name}/query_results/"
        }
    )
    query_execution_id = response['QueryExecutionId']
    while True:
        stats = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if status == 'SUCCEEDED':
        results = client.get_query_results(QueryExecutionId=query_execution_id)
        return results
    else:
        raise Exception(f"Query {query} failed with status {status}.")

# Function to update Athena table with data
def update_data_in_athena_table(df):
    df['id'] = df['id'].astype(str)
    df['score_happy'] = df['score_happy'].astype(float)
    df['score_calm'] = df['score_calm'].astype(float)
    df['score_alert'] = df['score_alert'].astype(float)
    df['score_sure'] = df['score_sure'].astype(float)
    df['score_vital'] = df['score_vital'].astype(float)
    create_table_if_not_exists(database, table, df)
    add_column_to_table(database, table, 'score_happy', 'DOUBLE')
    add_column_to_table(database, table, 'score_calm', 'DOUBLE')
    add_column_to_table(database, table, 'score_alert', 'DOUBLE')
    add_column_to_table(database, table, 'score_sure', 'DOUBLE')
    add_column_to_table(database, table, 'score_vital', 'DOUBLE')
    for i, row in df.iterrows():
        query = f"INSERT INTO {table} (id, text_content, score_happy, score_calm, score_alert, score_sure, score_vital) VALUES ('{row['id']}', '{row['text_content']}', {row['score_happy']}, {row['score_calm']}, {row['score_alert']}, {row['score_sure']}, {row['score_vital']})"
        execute_athena_query(query)

def get_sentiment_scores(tweets):
    batches = [tweets[i:i+50] for i in range(0, len(tweets), 50)]
    scores = []
    for batch in batches:
        prompt = "Please score the sentiment of the following tweets on a scale of -1 to 1, where -1 is very negative, 0 is neutral, and 1 is very positive:\n" + "\n".join(batch) + "\n---\n"
        response = openai.Completion.create(
            engine="davinci-003",
            prompt=prompt,
            temperature=0.5,
            max_tokens=1024,
            n=1,
            stop=None,
            timeout=60,
        )
        result = response.choices[0].text.strip()
        scores += json.loads(result)["scores"]
    return scores

# Function to update Athena table with data
def update_data_in_athena_table(df):
    df['id'] = df['id'].astype(str)
    df['score_happy'] = df['score_happy'].astype(float)
    df['score_calm'] = df['score_calm'].astype(float)
    df['score_alert'] = df['score_alert'].astype(float)
    df['score_sure'] = df['score_sure'].astype(float)
    df['score_vital'] = df['score_vital'].astype(float)
    create_table_if_not_exists(database, table, df)
    add_column_to_table(database, table, 'score_happy', 'DOUBLE')
    add_column_to_table(database, table, 'score_calm', 'DOUBLE')
    add_column_to_table(database, table, 'score_alert', 'DOUBLE')
    add_column_to_table(database, table, 'score_sure', 'DOUBLE')
    add_column_to_table(database, table, 'score_vital', 'DOUBLE')
    for i, row in df.iterrows():
        query = f"INSERT INTO {table} (id, text_content, score_happy, score_calm, score_alert, score_sure, score_vital) VALUES ('{row['id']}', '{row['text_content']}', {row['score_happy']}, {row['score_calm']}, {row['score_alert']}, {row['score_sure']}, {row['score_vital']})"
        execute_athena_query(query)

s3 = boto3.resource('s3')

def sentiment_analysis():
    bucket_name = 'your-bucket-name'
    tweets = []

    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        body = obj.get()['Body'].read().decode('utf-8')
        reader = csv.reader(body.splitlines())
        for row in reader:
            tweet = row[0]
            tweets.append(tweet)

    sentiment_scores = get_sentiment_scores(tweets)
    response = {'scores': sentiment_scores}

    return json.dumps(response)


