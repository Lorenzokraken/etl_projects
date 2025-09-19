# Silver ETL: Read/Write Parquet, No Globals, Cross-File Logic
from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import pandas as pd

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
bronze_container = blob_service_client.get_container_client("bronze")
silver_container = blob_service_client.get_container_client("silver")

def get_parquet_from_blob(container, blob_path):
	blob_data = container.download_blob(blob_path).readall()
	return pd.read_parquet(BytesIO(blob_data))

def push_parquet_to_blob(container, blob_path, df):
	parquet_bytes = BytesIO()
	df.to_parquet(parquet_bytes, index=False)
	parquet_bytes.seek(0)
	container.upload_blob(name=blob_path, data=parquet_bytes.getvalue(), overwrite=True)

def clean_hotels(df):
	col = next((c for c in df.columns if c.lower() == 'country'), None)
	if col:
		df = df[df[col] != 'XX'].copy()
	return df

def clean_customers(df):
	email_col = next((c for c in df.columns if c.lower() == 'email'), None)
	if email_col:
		df[email_col] = df[email_col].replace('', pd.NA)
		df['dq_email_null'] = df[email_col].isnull()
	if 'customer_id' in df.columns:
		df = df.drop_duplicates(subset=['customer_id']).copy()
	return df

def clean_rooms(df):
	if 'room_id' in df.columns:
		df = df.drop_duplicates(subset=['room_id']).copy()
	return df

def clean_bookings(df):
    if 'checkin_date' in df.columns and 'checkout_date' in df.columns:
        mask = df['checkin_date'] > df['checkout_date']
        df.loc[mask, ['checkin_date', 'checkout_date']] = df.loc[mask, ['checkout_date', 'checkin_date']].values
        df['dq_date_inverted'] = mask
    # Correzione qui:
    if 'total_amount' in df.columns:
        df['dq_amount_negative'] = df['total_amount'] < 0
        df['total_amount'] = df['total_amount'].apply(lambda x: x if x >= 0 else pd.NA)
    elif 'amount' in df.columns:
        df['dq_amount_negative'] = df['amount'] < 0
        df['amount'] = df['amount'].apply(lambda x: x if x >= 0 else pd.NA)
    valid_currency = ['EUR', 'USD', 'GBP']
    if 'currency' in df.columns:
        df['dq_currency_invalid'] = ~df['currency'].isin(valid_currency)
        df.loc[~df['currency'].isin(valid_currency), 'currency'] = pd.NA
    return df

def clean_payments(df, bookings_df):
	if 'booking_id' in df.columns and 'booking_id' in bookings_df.columns:
		df['dq_orphan'] = ~df['booking_id'].isin(bookings_df['booking_id'])
	else:
		df['dq_orphan'] = False
	if 'amount' in df.columns and 'total_amount' in df.columns:
		df['dq_over_amount'] = df['amount'] > df['total_amount']
	else:
		df['dq_over_amount'] = False
	valid_currency = ['EUR', 'USD', 'GBP']
	if 'currency' in df.columns:
		df['dq_currency_invalid'] = ~df['currency'].isin(valid_currency)
		df.loc[~df['currency'].isin(valid_currency), 'currency'] = pd.NA
	return df

def add_dq_missing_flags(df):
	for col in df.columns:
		df[f'dq_missing_{col}'] = df[col].isnull()
	return df

def main():
	today = os.getenv("INGESTION_DATE", pd.Timestamp.today().strftime('%Y-%m-%d'))
	files = ["hotels", "rooms", "customers", "bookings", "payments"]
	bronze_paths = {name: f"{today}/{name}.parquet" for name in files}
	dfs = {name: get_parquet_from_blob(bronze_container, path) for name, path in bronze_paths.items()}

	dfs["hotels"] = add_dq_missing_flags(clean_hotels(dfs["hotels"]))
	dfs["customers"] = add_dq_missing_flags(clean_customers(dfs["customers"]))
	dfs["rooms"] = add_dq_missing_flags(clean_rooms(dfs["rooms"]))
	dfs["bookings"] = add_dq_missing_flags(clean_bookings(dfs["bookings"]))
	dfs["payments"] = add_dq_missing_flags(clean_payments(dfs["payments"], dfs["bookings"]))

	for name, df in dfs.items():
		silver_path = f"{today}/{name}.parquet"
		push_parquet_to_blob(silver_container, silver_path, df)
		

if __name__ == "__main__":
    main()