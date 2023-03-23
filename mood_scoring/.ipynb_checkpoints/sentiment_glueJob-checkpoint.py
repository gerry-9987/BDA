import boto3
import json
import csv
import io
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit
from sentiments import get_sentiment_scores, execute_athena_query, create_table_if_not_exists, add_column_to_table, update_data_in_athena_table

# Set variables
bucket_name = "my_bucket"
file_name = "raw_data.csv"
database = "my_database"
table = "my_table"

# Initialize the context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Load data from S3
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
data = []
for obj in bucket.objects.all():
    key = obj.key
    body = obj.get()['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(body))
    for row in reader:
        data.append({"id": row["id"], "text_content": row["text_content"]})

# Create a Pandas dataframe from the S3 data
df_s3 = pd.DataFrame(data)

# Perform sentiment analysis
scores = get_sentiment_scores(df_s3["text_content"])

# Add the scores to the S3 data based on the ID
df_scores = pd.DataFrame(scores, columns=["score_happy", "score_calm", "score_alert", "score_sure", "score_vital"])
df_combined = pd.concat([df_s3, df_scores], axis=1)

# Update Athena table with the new data
update_data_in_athena_table(df_combined[["id", "text_content", "score_happy", "score_calm", "score_alert", "score_sure", "score_vital"]])

# End the job
job.commit()
