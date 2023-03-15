import pandas as pd

reddit_df = pd.read_csv('../cleaned_reddit.csv')
twitter_df = pd.read_csv('../cleaned_twitter.csv')

reddit_df.rename( columns=({ 'comment_id': 'id', 'comment_body': 'text'}), inplace=True )
twitter_df.rename( columns=({'user_location': 'location'}), inplace=True )

merged_df = pd.concat([twitter_df,reddit_df])

merged_df[['id','text','location','date']].to_csv("../merged.csv")