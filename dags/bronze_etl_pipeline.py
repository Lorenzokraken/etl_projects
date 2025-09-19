from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime
from io import BytesIO

#=============CONNESSIONE AL BLOB STORAGE====================
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY_1")

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = "bronze"
container_client = blob_service_client.get_container_client(container_name)


#===================AGGIUNTA DELLA COLONNA=====================


def upload_parquet_to_blob(local_path, blob_name):
    df = pd.read_csv(local_path)
    df['ingestion_date'] = datetime.today().strftime('%Y-%m-%d')
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    container_client.upload_blob(name=blob_name, data=parquet_buffer.getvalue(), overwrite=True)


def main():
    csv_files = ["hotels.csv", "rooms.csv", "customers.csv", "bookings.csv", "payments.csv"]
    today = datetime.today().strftime('%Y-%m-%d')
    for file in csv_files:
        parquet_blob_path = f"{today}/{file.replace('.csv', '.parquet')}"
        upload_parquet_to_blob(f"/opt/airflow/data/{file}", parquet_blob_path)

if __name__ == "__main__":
    main()