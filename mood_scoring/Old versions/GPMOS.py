import boto3
import pandas as pd
import requests
import json

# Set variables
database = "my_database"
table = "my_table"
new_column = "sentiment_scores"
bucket_name = "my_bucket"
file_name = "sentiment_scores.csv"
batch_size = 50
api_key = "my_openai_api_key"

# Athena query to retrieve tweets without sentiment scores
def get_tweets_without_scores():
    query = f"SELECT id, text_content FROM {table} WHERE score_happy IS NULL AND score_calm IS NULL AND score_alert IS NULL AND score_sure IS NULL AND score_vital IS NULL"
    df = athena_query_to_df(query)
    return df

# Function to call OpenAI's API and get sentiment scores
def get_sentiment_scores(tweet_list):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}',
    }
    data = {
        'prompt': 'There are 6 moods we want to determine. They are Calm, Alert, Sure, Vital, Kind, and Happy. A -1.00 on calm would mean the tweet is not calm at all while 1.00 will mean the tweet is very calm. Rate each of the 6 moods for the following tweets strictly between -1.00 and 1.00.',
        'max_tokens': 16,
        'temperature': 0.5,
        'n': 1,
        'stop': '\n',
        'inputs': tweet_list,
    }
    response = requests.post('https://api.openai.com/v1/completions', headers=headers, data=json.dumps(data))
    result = json.loads(response.text)['choices'][0]['text'].strip().split('\n')
    sentiment_scores = []
    for i in range(0, len(result), 7):
        sentiment_scores.append(result[i:i+7])
    return sentiment_scores

# Function to update Athena table with sentiment scores
def update_scores_in_athena_table(df):
    df['id'] = df['id'].astype(str)
    df['score_happy'] = df['score_happy'].astype(float)
    df['score_calm'] = df['score_calm'].astype(float)
    df['score_alert'] = df['score_alert'].astype(float)
    df['score_sure'] = df['score_sure'].astype(float)
    df['score_vital'] = df['score_vital'].astype(float)
    add_column_to_table(database, table, 'score_happy double, score_calm double, score_alert double, score_sure double, score_vital double')
    for i, row in df.iterrows():
        query = f"ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (id='{row['id']}') LOCATION 's3://{bucket_name}/{file_name}/{row['id']}'"
        execute_athena_query(query)
        query = f"UPDATE {table} SET score_happy = {row['score_happy']}, score_calm = {row['score_calm']}, score_alert = {row['score_alert']}, score_sure = {row['score_sure']}, score_vital = {row['score_vital']} WHERE id = '{row['id']}'"
        execute_athena_query(query)

# Function to execute Athena query and return results as pandas dataframe
def athena_query_to_df(query):
    """
    This function sends a SQL query to Athena and returns the result as a Pandas DataFrame.
    """
    # Set up the Athena client
    client = boto3.client('athena')
    # Get the query execution ID
    query_execution = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f's3://{bucket_name}/query_results/'
        }
    )
    query_execution_id = query_execution['QueryExecutionId']
    # Get the query results
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_execution_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )
    # Convert the results to a DataFrame
    data = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data.append([field.get('VarCharValue', '') for field in row['Data']])
    df = pd.DataFrame(data[1:], columns=data[0])
    return df
# Function to update Athena table with sentiment scores

def add_column_to_table(database, table, column_name, column_type):
    query = (
        f"ALTER TABLE {database}.{table} "
        f"ADD COLUMN {column_name} {column_type}"
    )
    execution_id = execute_athena_query(query)
    print(f"Execution ID: {execution_id}")
    return execution_id

# Get tweets without sentiment scores
tweets = get_tweets_without_scores(database, table)

# Get sentiment scores in batches of 50
batch_size = 50
for i in range(0, len(tweets), batch_size):
    batch = tweets[i:i+batch_size]
    scores = get_sentiment_scores(batch, sentiment_api_key)
    
    # Add sentiment scores to dataframe
    for score in scores:
        tweet_id = score["id"]
        tweet_scores = [tweet_id]
        for mood in ["score_happy", "score_calm", "score_alert", "score_sure", "score_vital", "score_kind"]:
            tweet_scores.append(score[mood])
        tweets.loc[tweets['id'] == tweet_id, ["score_happy", "score_calm", "score_alert", "score_sure", "score_vital", "score_kind"]] = tweet_scores[1:]

    # Add sentiment scores to Athena table
    add_column_to_table(database, table, "score_happy", "FLOAT")
    add_column_to_table(database, table, "score_calm", "FLOAT")
    add_column_to_table(database, table, "score_alert", "FLOAT")
    add_column_to_table(database, table, "score_sure", "FLOAT")
    add_column_to_table(database, table, "score_vital", "FLOAT")
    add_column_to_table(database, table, "score_kind", "FLOAT")
    
    # Write updated dataframe to S3
    csv_buffer = StringIO()
    tweets.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    
    # Add data from S3 to Athena table
    query = (
        f"ALTER TABLE {database}.{table} "
        f"ADD PARTITION (dt='{datetime.date.today().strftime('%Y%m%d')}') "
        f"LOCATION '{s3_output}{file_name}'"
    )
    execution_id = execute_athena_query(query)
    print(f"Execution ID: {execution_id}")



