import os
import requests
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# Reuirements
repo_url = 'https://raw.githubusercontent.com/sfrechette/adventureworks-neo4j/master/data/'
owner = "sfrechette"
repo = "adventureworks-neo4j"
path = "data"
bucket_name = 'shop_data_bucket'
dataset_id = 'shop_data_bucket_dataset'

# 1. Defining environment variable GOOGLE_APPLICATION_CREDENTIALS
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Programming/Projekt Onwelo/key/key-bucket.json'

# 2. Extracting file names from github repository

# Function to list names of files from github repo
def list_files_from_github_repo(owner, repo, path):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    response = requests.get(url)

    if response.status_code == 200:
        files = [item['name'] for item in response.json() if item['type'] == 'file']
        return files
    else:
        print(f"Error {response.status_code}: {response.text}")
        return []

files = list_files_from_github_repo(owner, repo, path)

# 3. Cloud storage client configuration and file upload
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Downloading files and loading them to the bucket
for file in files:
    response = requests.get(repo_url + file)
    blob = bucket.blob(file)
    blob.upload_from_string(response.content.decode('latin-1'), content_type='text/csv')

# 4. Extracting files from Cloud Storage, creating tables in BigQuery and loading the data to them

# BigQuery client configuration
bq_client = bigquery.Client()

def generate_bq_schema_from_csv(dataframe):

    # Translate pandas dtypes to BigQuery dtypes
    type_translation = {
        'int64': 'INT64',
        'float64': 'FLOAT64',
        'bool': 'BOOL',
        'object': 'STRING',
    }
    
    schema = []
    for column, dtype in dataframe.dtypes.items():
        if dtype.name in type_translation:
            schema.append(bigquery.SchemaField(column, type_translation[dtype.name]))
        else:
            schema.append(bigquery.SchemaField(column, 'STRING'))
    
    return schema


for file in files:
    table_id = file.replace('.csv', '')
    full_table_id = f'{dataset_id}.{table_id}'
    uri = f'gs://{bucket_name}/{file}'
    df = pd.read_csv(uri)
    df['inserted_at'] = datetime.utcnow()
    df_schema = generate_bq_schema_from_csv(df)
    df_schema.append( bigquery.SchemaField('inserted_at', 'TIMESTAMP'))

    job_config = bigquery.LoadJobConfig(
        schema=df_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    load_job.result()