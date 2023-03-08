import pandas as pd
import pyarrow

# Read in the two CSV files
df1 = pd.read_csv('member_profile.csv')
df2 = pd.read_csv('transactions.csv')

# Merge the two DataFrames based on the member_id column
merged_df = pd.merge(df1, df2, on=['member_id'])

# Print the merged DataFrame
print(merged_df)

merged_df.to_parquet('output.parquet')
