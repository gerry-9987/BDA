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

# Creating the corpus
data_lemmatized = df_s3['text_content'].tolist()
data_lemmatized = [str_to_list(text) for text in data_lemmatized]

# Create Dictionary
id2word = corpora.Dictionary(data_lemmatized)

# Filter out tokens that appear in only 1 documents and appear in more than 90% of the documents
id2word.filter_extremes(no_below=2, no_above=0.9)

# Create Corpus
texts = data_lemmatized

# Term Document Frequency
corpus = [id2word.doc2bow(text) for text in texts]

# Build LDA Model
ntopics = 10

lda_model = gensim.models.LdaMulticore(corpus=corpus,
                                       id2word=id2word,
                                       num_topics=ntopics, 
                                       random_state=100,
                                       chunksize=100,
                                       passes=10,
                                       per_word_topics=True)

doc_num, topic_num, prob = [], [], []
lda_model.get_document_topics(corpus)
for n in range(len(df_s3)):
    get_document_topics = lda_model.get_document_topics(corpus[n])
    doc_num.append(n)
    sorted_doc_topics = Sort_Tuple(get_document_topics)
    topic_num.append(sorted_doc_topics[0][0])
    prob.append(sorted_doc_topics[0][1])
    
df_s3['Doc'] = doc_num
df_s3['Topic'] = topic_num
df_s3['Probability'] = prob

# Perform sentiment analysis
scores = get_sentiment_scores(df_s3["text_content"])

# Add the scores to the S3 data based on the ID
df_scores = pd.DataFrame(scores, columns=["score_happy", "score_calm", "score_alert", "score_sure", "score_vital"])
df_combined = pd.concat([df_s3, df_scores], axis=1)

# Update Athena table with the new data
update_data_in_athena_table(df_combined[["id", "text_content", "score_happy", "score_calm", "score_alert", "score_sure", "score_vital"]])

# End the job
job.commit()
