import pandas as pd
from datetime import datetime, timedelta

# Generate test tweets
test_tweets = generate_test_tweets(5)

# Calculate sentiment scores for the tweets
sentiments = calculate_sentiments(test_tweets)

# Add the sentiments to the DataFrame
df = pd.DataFrame(test_tweets)
add_column_to_table(df, 'sentiment', sentiments)

# Update the scores in the Athena table
update_scores_in_athena_table(df)

# Get tweets without scores from the Athena table
tweets_without_scores = get_tweets_without_scores()

# Check that the tweets without scores are the same as the test tweets
assert tweets_without_scores.equals(df[['id', 'text']])

# Test the API calling function
api_key = 'your_api_key'
tweets = call_twitter_api('python', api_key)

# Check that the returned tweets are a DataFrame with the expected columns
assert isinstance(tweets, pd.DataFrame)
assert set(tweets.columns) == {'id', 'text', 'created_at'}

# Print the DataFrame that will be added to the Athena table
print(df)
