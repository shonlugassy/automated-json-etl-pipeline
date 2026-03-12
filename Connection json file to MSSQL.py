
#connection and load to MSSQL without the nesting data
import pyodbc
import pandas as pd
import json
from sqlalchemy import create_engine

# Connection details for MSSQL Server with Windows authentication
server = 'DESKTOP-I8DIQH8\\SQLEXPRESS' # +++++++++++++++ chnage the server name accordingly ++++++++++++++
database = 'Json Practice' # ++++++++++++++++++ change the database name accordiingly +++++++++++++

# Create a connection string for SQLAlchemy
connection_string = f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes'

# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Load the JSON data
data = []   # +++++++++++++++++++  change the path accordingly (line 20) to the desired json file +++++++++++++++
with open(r'C:\Users\shonl\Desktop\udemy data analyst course\Youtube Data Analyst Course\JsonFilespractice\companies_us_sample_10000_records.json', 'r') as file:
    for line in file:
        data.append(json.loads(line))

# Normalize the nested JSON data
df = pd.json_normalize(data, sep='_')

# Function to flatten array columns
def flatten_array_column(df, column_name, fields):
    if column_name in df.columns:
        # Expand the first entry of the list
        for field in fields:
            df[f'{column_name}_{field}'] = df[column_name].apply(
                lambda x: x[0].get(field) if isinstance(x, list) and len(x) > 0 else None
            )
        # Drop the original array column
        df = df.drop(columns=[column_name])
    return df

# Handling the 'specialties' column separately if it's just a list of strings
if 'specialties' in df.columns:
    df['specialties'] = df['specialties'].apply(lambda x: ', '.join(x) if isinstance(x, list) else None)

# Insert the DataFrame into the database
df.to_sql('json_file', con=engine, if_exists='replace', index=False) #++++++ the'json_file' is the name that will be in the database on MSSQL

print("Data inserted successfully!")
