import pandas as pd
import pyarrow as pa

# Read in the two CSV files in chunks and specify data types and columns to read
df1 = pd.read_csv('member_profile.csv', usecols=['member_id', 'member_name'], dtype={
                  'member_id': 'int32', 'member_name': 'category'}, chunksize=100000)
df2 = pd.read_csv('transactions.csv', usecols=['member_id', 'transaction_type', 'created_date', 'price'], dtype={
                  'member_id': 'int32', 'transaction_type': 'category', 'created_date': 'datetime64[ns]', 'price': 'float32'}, chunksize=100000)


# Merge the two DataFrames in chunks and append them to a list
merged_dfs = []
for chunk1, chunk2 in zip(df1, df2):
    merged_df = pd.merge(chunk1, chunk2, on=['member_id'])
    merged_dfs.append(merged_df)

# Concatenate the merged DataFrames into a single DataFrame
merged_df = pd.concat(merged_dfs)

# Write the merged DataFrame to a Parquet file
merged_df.to_parquet('output.parquet', engine='pyarrow')

#####
# In this optimized version, we use the usecols parameter to read only the columns we need and the dtype parameter to specify the data types of the columns.
# #We also read in the CSV files in chunks using the chunksize parameter and merge the DataFrames in chunks.
# #Finally, we write the merged DataFrame to a Parquet file using the to_parquet() function.
