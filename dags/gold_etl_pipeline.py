"""
Gold Layer ETL - Test KPI con Debug

Questo script trasforma i dati Silver in KPI Gold e salva i risultati su Azure Blob Storage.
Aggiunti commenti dettagliati e debug sulle colonne per facilitare la diagnosi di errori.
"""

import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv
import os
from datetime import datetime

# Carica variabili d'ambiente
load_dotenv()

# Connessione a Blob Storage
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY_1")
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
silver_container = blob_service_client.get_container_client("silver")
gold_container = blob_service_client.get_container_client("gold")

def get_parquet_from_blob(container, blob_path):
    """Scarica e carica un file Parquet da Azure Blob Storage."""
    try:
        blob_data = container.download_blob(blob_path).readall()
        return pd.read_parquet(BytesIO(blob_data))
    except Exception as e:
        print(f"Errore nella lettura di {blob_path}: {e}")
        return None

def push_parquet_to_blob(container, blob_path, df):
    """Scrive un DataFrame come Parquet su Azure Blob Storage."""
    try:
        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)
        container.upload_blob(name=blob_path, data=parquet_bytes.getvalue(), overwrite=True)
        print(f"Scrittura completata di {blob_path}")
    except Exception as e:
        print(f"Errore nella scrittura di {blob_path}: {e}")

def main():
    """
    Trasforma i dati Silver in KPI Gold e salva i risultati.
    Debug: stampa colonne e tipi dei DataFrame caricati.
    """
    today = os.getenv("INGESTION_DATE", datetime.today().strftime('%Y-%m-%d'))
    print(f"Elaborazione dati per la data: {today}")

    # Definisci i file da caricare
    silver_files = ["hotels", "rooms", "customers", "bookings", "payments"]
    silver_paths = {name: f"{today}/{name}.parquet" for name in silver_files}

    # Carica i DataFrame e stampa info debug
    dfs = {}
    for name, path in silver_paths.items():
        print(f"Caricamento dati {name}...")
        dfs[name] = get_parquet_from_blob(silver_container, path)
        if dfs[name] is not None:
            print(f"  Caricati {len(dfs[name])} record da {name}")
            print(f"  Colonne: {list(dfs[name].columns)}")
            print(f"  Tipi: {dfs[name].dtypes}")
        else:
            print(f"  Errore nel caricamento di {name}")

    # Estrai i DataFrame
    hotels_df = dfs.get("hotels")
    rooms_df = dfs.get("rooms")
    customers_df = dfs.get("customers")
    bookings_df = dfs.get("bookings")
    payments_df = dfs.get("payments")

    # Inizializza risultati KPI
    kpi_results = {}

    # Salva bookings.parquet (solo colonne richieste, con nights calcolato)
    try:
        valid_bookings_for_ml = bookings_df[
            (bookings_df['dq_date_inverted'] == False) &
            (bookings_df['dq_amount_negative'] == False) &
            (bookings_df['dq_currency_invalid'] == False) &
            (bookings_df['status'] == 'confirmed') &
            (bookings_df['dq_missing_total_amount'] == False) &
            (bookings_df['dq_missing_booking_id'] == False) &
            (bookings_df['dq_missing_checkin_date'] == False)
        ].copy()
        # Calcola la colonna nights
        valid_bookings_for_ml['nights'] = (
            pd.to_datetime(valid_bookings_for_ml['checkout_date']) - pd.to_datetime(valid_bookings_for_ml['checkin_date'])
        ).dt.days
        # Seleziona solo le colonne richieste
        columns_needed = [
            "room_id",
            "dq_missing_customer_id",
            "dq_amount_negative",
            "source",
            "checkout_date",
            "booking_id",
            "dq_date_inverted",
            "total_amount",
            "checkin_date",
            "nights",
            "dq_currency_invalid",
            "customer_id",
            "status"
        ]
        valid_bookings_for_ml = valid_bookings_for_ml[columns_needed]
        gold_path = f"{today}/bookings.parquet"
        push_parquet_to_blob(gold_container, gold_path, valid_bookings_for_ml)
        print(f"  bookings.parquet per ML salvato con {len(valid_bookings_for_ml)} righe e colonne pulite")
    except Exception as e:
        print(f"  Errore nel salvataggio bookings.parquet per ML: {e}")

    # Salva anche rooms.parquet (senza colonne dq_*)
    try:
        rooms_clean = rooms_df[[col for col in rooms_df.columns if not col.startswith('dq_')]].copy()
        gold_path = f"{today}/rooms.parquet"
        push_parquet_to_blob(gold_container, gold_path, rooms_clean)
        print(f"  rooms.parquet pulito salvato con {len(rooms_clean)} righe")
    except Exception as e:
        print(f"  Errore nel salvataggio rooms.parquet pulito: {e}")

    # KPI 1: Daily Revenue
    print("Calcolo KPI Daily Revenue...")
    try:
        # Debug: verifica presenza colonne
        print("Colonne bookings_df:", bookings_df.columns)
        # Filtra prenotazioni valide
        valid_bookings_df = bookings_df[
            (bookings_df['dq_date_inverted'] == False) &
            (bookings_df['dq_amount_negative'] == False) &
            (bookings_df['dq_currency_invalid'] == False) &
            (bookings_df['status'] == 'confirmed') &
            (bookings_df['dq_missing_total_amount'] == False) &
            (bookings_df['dq_missing_booking_id'] == False) &
            (bookings_df['dq_missing_checkin_date'] == False)
        ].copy()
        daily_revenue_df = valid_bookings_df.groupby('checkin_date').agg({
            'total_amount': 'sum',
            'booking_id': 'count'
        }).reset_index()
        daily_revenue_df.columns = ['date', 'gross_revenue', 'bookings_count']
        kpi_results["daily_revenue"] = daily_revenue_df
        print(f"  Generati {len(daily_revenue_df)} record di ricavi giornalieri")
    except Exception as e:
        print(f"  Errore nel calcolo del KPI Daily Revenue: {e}")

    # KPI 2: Cancellation Rate by Source
    print("Calcolo KPI Cancellation Rate by Source...")
    try:
        cancellation_df = bookings_df.groupby('source').agg({
            'booking_id': 'count',
            'status': lambda x: (x == 'cancelled').sum()
        }).reset_index()
        cancellation_df.columns = ['source', 'total_bookings', 'cancelled']
        cancellation_df['cancellation_rate_pct'] = (
            cancellation_df['cancelled'] / cancellation_df['total_bookings'] * 100
        )
        kpi_results["cancellation_rate"] = cancellation_df
        print(f"  Generati {len(cancellation_df)} record di tasso di cancellazione")
    except Exception as e:
        print(f"  Errore nel calcolo del KPI Cancellation Rate: {e}")

    # KPI 3: Collection Rate
    print("Calcolo KPI Collection Rate...")
    try:
        collection_df = bookings_df.merge(payments_df, on='booking_id', how='inner', suffixes=('_booking', '_payment'))
        print("Colonne collection_df:", collection_df.columns)
        print("Prime righe collection_df:", collection_df.head())
        # Debug: stampa valori unici e conteggi per le colonne di filtro
        for col in ['dq_amount_negative', 'dq_currency_invalid', 'dq_orphan', 'dq_over_amount', 'dq_missing_total_amount', 'dq_missing_amount']:
            if col in collection_df.columns:
                print(f"Valori unici {col}: {collection_df[col].unique()}")
                print(f"Conteggio True {col}: {(collection_df[col]==True).sum()} | False: {(collection_df[col]==False).sum()}")
            else:
                print(f"Colonna {col} NON presente in collection_df")
        valid_collection_df = collection_df[
            (collection_df['dq_amount_negative'] == False) &
            (collection_df['dq_currency_invalid_booking'] == False) &
            (collection_df['dq_currency_invalid_payment'] == False) &
            (collection_df['dq_orphan'] == False) &
            (collection_df['dq_over_amount'] == False) &
            (collection_df['dq_missing_total_amount'] == False) &
            (collection_df['dq_missing_amount'] == False)
        ].copy()
        print(f"Righe dopo filtro: {len(valid_collection_df)}")
        print("Prime righe valid_collection_df:", valid_collection_df.head())
        collection_result_df = valid_collection_df.groupby('hotel_id').agg({
            'total_amount': 'sum',
            'amount': 'sum'
        }).reset_index()
        collection_result_df.columns = ['hotel_id', 'total_bookings_value', 'total_payments_value']
        collection_result_df['collection_rate'] = (
            collection_result_df['total_payments_value'] / collection_result_df['total_bookings_value']
        )
        print(f"Righe collection_result_df: {len(collection_result_df)}")
        print("Prime righe collection_result_df:", collection_result_df.head())
        kpi_results["collection_rate"] = collection_result_df
        print(f"  Generati {len(collection_result_df)} record di tasso di incasso")
    except Exception as e:
        print(f"  Errore nel calcolo del KPI Collection Rate: {e}")

    # KPI 4: Overbooking Alerts
    print("Calcolo KPI Overbooking Alerts...")
    try:
        overbooking_df = bookings_df.merge(bookings_df, on='room_id', suffixes=('_1', '_2'))
        print("Colonne overbooking_df:", overbooking_df.columns)
        overlap_condition = (
            overbooking_df[['checkin_date_1', 'checkin_date_2']].max(axis=1) < 
            overbooking_df[['checkout_date_1', 'checkout_date_2']].min(axis=1)
        )
        not_same_booking = overbooking_df['booking_id_1'] != overbooking_df['booking_id_2']
        valid_booking_1 = (
            (overbooking_df['dq_date_inverted_1'] == False) &
            (overbooking_df['dq_amount_negative_1'] == False) &
            (overbooking_df['dq_currency_invalid_1'] == False)
        )
        valid_booking_2 = (
            (overbooking_df['dq_date_inverted_2'] == False) &
            (overbooking_df['dq_amount_negative_2'] == False) &
            (overbooking_df['dq_currency_invalid_2'] == False)
        )
        overbooking_alerts_df = overbooking_df[
            overlap_condition & 
            not_same_booking & 
            valid_booking_1 & 
            valid_booking_2
        ][[
            'room_id', 'booking_id_1', 'booking_id_2', 'checkin_date_1', 'checkin_date_2',
            'checkout_date_1', 'checkout_date_2'
        ]].copy()
        overbooking_alerts_df['overlap_start'] = overbooking_alerts_df[[
            'checkin_date_1', 'checkin_date_2'
        ]].max(axis=1)
        overbooking_alerts_df['overlap_end'] = overbooking_alerts_df[[
            'checkout_date_1', 'checkout_date_2'
        ]].min(axis=1)
        overbooking_alerts_df = overbooking_alerts_df[[
            'room_id', 'booking_id_1', 'booking_id_2', 'overlap_start', 'overlap_end'
        ]]
        kpi_results["overbooking_alerts"] = overbooking_alerts_df
        print(f"  Generati {len(overbooking_alerts_df)} avvisi di overbooking")
    except Exception as e:
        print(f"  Errore nel calcolo del KPI Overbooking Alerts: {e}")

    # KPI 5: Customer Value
    print("Calcolo KPI Customer Value...")
    try:
        print("Colonne bookings_df:", bookings_df.columns)
        valid_bookings_df = bookings_df[
            (bookings_df['dq_amount_negative'] == False) &
            (bookings_df['dq_currency_invalid'] == False) &
            (bookings_df['dq_missing_total_amount'] == False) &
            (bookings_df['dq_missing_customer_id'] == False)
        ].copy()
        customer_value_df = valid_bookings_df.groupby('customer_id').agg({
            'booking_id': 'count',
            'total_amount': 'sum'
        }).reset_index()
        customer_value_df.columns = ['customer_id', 'bookings_count', 'revenue_sum']
        customer_value_df['avg_ticket'] = (
            customer_value_df['revenue_sum'] / customer_value_df['bookings_count']
        )
        kpi_results["customer_value"] = customer_value_df
        print(f"  Generati {len(customer_value_df)} record di valore cliente")
    except Exception as e:
        print(f"  Errore nel calcolo del KPI Customer Value: {e}")

    # Salva tutti i KPI nel livello Gold
    print("Salvataggio risultati KPI nel livello Gold...")
    for name, df in kpi_results.items():
        if df is not None and not df.empty:
            gold_path = f"{today}/{name}.parquet"
            push_parquet_to_blob(gold_container, gold_path, df)
        else:
            print(f"  Saltato {name} - nessun dato da salvare")

    print("Trasformazioni livello Gold completate!")

def gold_etl_task():
    main()
