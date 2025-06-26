import pandas as pd

# File paths
tqqq_file = 'TQQQ.CSV'
qqq_file = 'QQQ.CSV'
output_file = 'Merge.CSV'

# Read the CSV files
try:
    tqqq_df = pd.read_csv(tqqq_file)
    qqq_df = pd.read_csv(qqq_file)
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit()

# Ensure Date columns are in datetime format for consistency
tqqq_df['Date'] = pd.to_datetime(tqqq_df['Date'])
qqq_df['Date'] = pd.to_datetime(qqq_df['Date'])

# Rename NAV columns to distinguish between TQQQ and QQQ
tqqq_df = tqqq_df.rename(columns={'NAV': 'TQQQ_NAV'})
qqq_df = qqq_df.rename(columns={'NAV': 'QQQ_NAV'})

# Merge the DataFrames, keeping all dates from TQQQ.CSV (left join)
merged_df = pd.merge(tqqq_df, qqq_df, on='Date', how='left')

# Sort by Date in descending order (newest first)
merged_df = merged_df.sort_values('Date', ascending=False)

# Write the merged DataFrame to a new CSV
merged_df.to_csv(output_file, index=False)
print(f"Merged data written to '{output_file}'")

# Display the first few rows (optional)
print("\nFirst 5 rows of merged data (descending order):")
print(merged_df.head())