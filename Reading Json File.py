import json
import pandas as pd

# Load the JSON data
data = []
with open('D:\\companies\\companies_us_sample_10000_records.json', 'r') as file: # +++++++++ change the path accordingly
    for line in file:
        data.append(json.loads(line))

# Normalize the nested JSON data
df = pd.json_normalize(data, sep='_')

# Set Pandas options to display all rows, all columns, and full content
pd.set_option('display.max_rows', None)     # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.set_option('display.max_colwidth', None) # Display full content of each column

# View the DataFrame
print(df.head())

