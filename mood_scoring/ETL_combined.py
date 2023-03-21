import boto3
import pandas as pd
from io import StringIO
import requests
from sentiment_score.py import make_api_call
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit

import openai
import pandas as pd
import json

s3 = boto3.resource('s3')
client = boto3.client('s3')
    
def get_csv_from_s3(bucket, key):
    obj = s3.Object(bucket, key)
    body = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(body))
    
def put_csv_to_s3(dataframe, bucket, key):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    
def update_csv_with_scores(bucket, key):
    df = get_csv_from_s3(bucket, key)
    scores_json_list = make_api_call(df, 10)
    scores_list = [json.loads(scores_json) for scores_json in scores_json_list]
    scores_df = pd.DataFrame(scores_list)
    updated_df = pd.concat([df, scores_df], axis=1)

    # @Steph I think you can add your code here for topic modelling and append to the updated_df before putting to cleaned s3 bucket

    put_csv_to_s3(updated_df, bucket, key)
        
def main():
    source_bucket = "source-bucket"
    source_key = "source-data.csv"
    dest_bucket = "clean-bucket"
    dest_key = "clean-data.csv"

    # ===== SENTIMENT SCORING =====
    openai.api_key = "YOUR_OPENAI_API_KEY"
    update_csv_with_scores(source_bucket, source_key)
    print("Data successfully updated and moved to clean bucket.")
    
    # Uncomment the following line to delete the source data after moving it to the clean bucket
    # client.delete_object(Bucket=source_bucket, Key=source_key)
    
if __name__ == "__main__":
    main()
