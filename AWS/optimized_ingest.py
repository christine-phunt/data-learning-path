import pandas as pd
import pyarrow as pa
import time


def ingest():
    start_time = time.time()  # get current time
    try:
        # Read in the two CSV files in chunks and specify data types and columns to read
        df1 = pd.read_csv('data/member_profile.csv', usecols=['member_id', 'member_name'], dtype={
            'member_id': 'int32', 'member_name': 'category'}, chunksize=100000)
        df2 = pd.read_csv('data/generated_transactions.csv', usecols=['member_id', 'transaction_type', 'created_date', 'price'], parse_dates=['created_date'], dtype={
            'member_id': 'int32', 'transaction_type': 'category', 'price': 'float32'}, chunksize=100000)

        # Merge the two DataFrames in chunks and append them to a list
        merged_dfs = []
        for chunk1, chunk2 in zip(df1, df2):
            merged_df = pd.merge(chunk1, chunk2, on=['member_id'])
            merged_dfs.append(merged_df)

        # Concatenate the merged DataFrames into a single DataFrame
        merged_df = pd.concat(merged_dfs)

        # Write the merged DataFrame to a Parquet file
        merged_df.to_parquet('data/output.parquet', engine='pyarrow')

    except FileNotFoundError as e:
        print("File not found error:", e)

    except pd.errors.EmptyDataError as e:
        print("Empty data error:", e)

    except Exception as e:
        print("Unknown error:", e)

    finally:
        print(" --------------- ")
        print("Execution completed. File converted to parquet. See `output.parquet`")
        print(" --------------- ")
        end_time = time.time()  # get current time again

        elapsed_time = end_time - start_time
        print(f"Processing took {elapsed_time:.2f} seconds.")

    #####
    # In this optimized version, we use the usecols parameter to read only the columns we need and the dtype parameter to specify the data types of the columns.
    # #We also read in the CSV files in chunks using the chunksize parameter and merge the DataFrames in chunks.
    # #Finally, we write the merged DataFrame to a Parquet file using the to_parquet() function.


ingest()
