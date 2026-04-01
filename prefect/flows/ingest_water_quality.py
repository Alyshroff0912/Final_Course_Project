import io
import os
from pathlib import Path
from datetime import date

import pandas as pd
import requests
from prefect import flow, task
from prefect_gcp import GcpCredentials
from google.cloud import storage, bigquery

PROJECT_ID    = "final-course-project-491204"        
BUCKET_NAME   = "final_project_water_quality_raw"    
BQ_DATASET    = "final_project_water_quality_dataset" 
CRED_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./keys/cred_keys.json")            

DATASETS = {
    # Main dataset — violations, lead/copper, scores   
    "zipcheckup":  "https://raw.githubusercontent.com/artakulov/us-water-quality-data/main/zipcheckup-water-quality.csv",
    # Consumer Confidence Report data, 1,062 systems
    "ccr_enriched": "https://raw.githubusercontent.com/artakulov/us-water-quality-data/main/ccr-enriched.csv",
    #L3 composite risk metrics
    "l3_metrics":  "https://raw.githubusercontent.com/artakulov/us-water-quality-data/main/l3-metrics.csv",
}

@task
def download_data(name: str, url: str) -> pd.DataFrame:
    print (f"Downloading {name} dataset from {url}...")
    response = requests.get(url)
    response.raise_for_status()  # Check for HTTP errors
    df = pd.read_csv(io.StringIO(response.text))
    print(f"Downloaded {name} dataset with {len(df)} records, {len(df.columns)} columns.")
    return df

@task
def clean_data(name: str, df: pd.DataFrame) -> pd.DataFrame:
    # standarized column names to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # fix only the known mixed-type column (ccr_violation_count has strings + floats)
    NUMERIC_COLUMNS = ['ccr_violation_count']
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    #adding ingestion date column to keep track of when data was ingested
    df['ingestion_date'] = pd.to_datetime('today').date()
    print(f"{name} dataset cleaned, {len(df):,} rows")
    return df

@task
def save_as_parquet(name: str, df: pd.DataFrame) -> Path:
    local_path = Path(f"/tmp/water_quality_{name}.parquet")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(local_path, index=False)
    print(f"{name} dataset saved as parquet to {local_path}")
    return local_path

@task
def upload_to_gcs(local_path: Path,name: str) -> str:
    print(f"Uploading {name} dataset to GCS from {local_path}...")
    gcp_path = f"raw/{date.today()}/{name}.parquet"

    client = storage.Client.from_service_account_json(CRED_PATH)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcp_path)
    blob.upload_from_filename(local_path)

    print (f"uploaded {name} dataset to gs://{BUCKET_NAME}/{gcp_path}")
    
    return gcp_path


@task
def load_to_bigquery(name: str, gcp_path: str):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.raw_{name}"

    client = bigquery.Client.from_service_account_json(CRED_PATH)

    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.PARQUET,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    url = f"gs://{BUCKET_NAME}/{gcp_path}"
    load_job = client.load_table_from_uri(url, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns into {table_id}.")

@flow   
def ingest_water_quality_data():
    for name, url in DATASETS.items():
        df = download_data(name, url)
        df_clean = clean_data(name, df)
        parquet = save_as_parquet(name, df_clean)
        gcp_path = upload_to_gcs(parquet, name)
        load_to_bigquery(name, gcp_path)

        print("All datasets ingested successfully!")

if __name__ == "__main__":
    ingest_water_quality_data()