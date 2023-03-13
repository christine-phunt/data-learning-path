import time
import pandas as pd
import pyarrow
start_time = time.time()  # get current time

try:
    # Read in the two CSV files
    df1 = pd.read_csv('member_profile.csv')
    df2 = pd.read_csv('generated_transactions.csv')

    # Merge the two DataFrames based on the member_id column
    merged_df = pd.merge(df1, df2, on=['member_id'])

    merged_df.to_parquet('output.parquet')

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
