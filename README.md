

# ETL & ML Pipeline - Overview



## Struttura della pipeline

<pre>

# GlobalStay Hotels - Pipeline Architecture
<span style="color:#0074D9; font-weight:bold;">💻 PC Locale</span>         <span style="color:#FF851B; font-weight:bold;">          🛠️ Airflow</span>           <span style="color:#2ECC40; font-weight:bold;">        ☁️ S3 Bucket</span>         <span style="color:#B10DC9; font-weight:bold;">      Databricks</span>
📊 DATA SOURCES        🛠️ AIRFLOW ORCHESTRATION           ☁️ AZURE STORAGE           🔮 DATABRICKS
                       
┌──────────────┐       ┌──────────────────────────────-───┐   ┌─────────────────┐       ┌──────────────┐
│ File CSV     │       │         DAG AIRFLOW              │   │ Azure Blob      │       │ Databricks   │
│ (Locale)     │       │                                  │   │ Storage         │       │ Workspace    │
│              │       │  ┌─────────────────────────────┐ │   │                 │       │              │
│ • hotels.csv │  ───► │  │     bronze_etl_task()       │ │ ──► ┌─────────────┐ │       │              │
│ • rooms.csv  │       │  │                             │ │   │ │   BRONZE    │ │       │              │
│ • customers  │       │  │ • Legge i file CSV          │ │   │ │   LAYER     │ │       │              │
│ • bookings   │       │  │ • Aggiunge ingestion_date   │ │   │ │             │ │       │              │
│ • payments   │       │  │ • Converte in Parquet       │ │   │ │ Dati grezzi │ │       │              │
│              │       │  │ • Carica su Azure           │ │   │ │ + metadata  │ │       │              │
└──────────────┘       │  └─────────────────────────────┘ │   │ └─────────────┘ │       │              │
                       │              │                   │   │        │        │       │              │
                       │              ▼                   │   │        ▼        │       │              │
                       │  ┌─────────────────────────────┐ │   │ ┌─────────────┐ │  ───► │ ┌──────────┐ │
                       │  │     silver_etl_task()       │ │ ◄─┤ │   SILVER    │ │       │ │ ML Model │ │
                       │  │                             │ │   │ │   LAYER     │ │       │ │          │ │
                       │  │ • Scarica da Bronze         │ │   │ │             │ │       │ │ • Prezzo │ │
                       │  │ • Applica regole DQ         │ │   │ │ Dati puliti │ │       │ │   Pred.  │ │
                       │  │ • Aggiunge colonne dq_*     │ │   │ │ + flag DQ   │ │       │ │ • Report │ │
                       │  │ • Carica dati puliti        │ │   │ │             │ │       │ │ • Grafici│ │
                       │  └─────────────────────────────┘ │   │ └─────────────┘ │       │ └──────────┘ │
                       │              │                   │   │        │        │       │              │
                       │              ▼                   │   │        ▼        │       │              │
                       │  ┌─────────────────────────────┐ │   │ ┌─────────────┐ │       │              │
                       │  │      gold_etl_task()        │ │ ◄─┤ │    GOLD     │ │       │              │
                       │  │                             │ │   │ │   LAYER     │ │       │              │
                       │  │ • Scarica da Silver         │ │   │ │             │ │       │              │
                       │  │ • Calcola KPI di business   │ │   │ │ Dati pronti │ │       │              │
                       │  │ • Daily Revenue             │ │   │ │ per analisi │ │       │              │
                       │  │ • Cancellation Rates        │ │   │ │             │ │       │              │
                       │  │ • Customer Value            │ │   │ │ • Daily Rev │ │       │              │
                       │  │ • Collection Rate           │ │   │ │ • Cancel %  │ │       │              │
                       │  │ • Overbooking Alerts        │ │   │ │ • Cust Val  │ │       │              │
                       │  │ • Carica i KPI              │ │   │ │ • Overbook  │ │       │              │
                       │  └─────────────────────────────┘ │   │ └─────────────┘ │       │              │
                       │                                  │   │                 │       │              │
                       │  [Possibile estensione futura]   │   │                 │       │              │
                       │  ┌─────────────────────────────┐ │   │                 │       │              │
                       │  │   databricks_ml_task()      │ │   │                 │       │              │
                       │  │                             │ │   │                 │       │              │
                       │  │ • Avvia notebook ML         │ │   │                 │       │              │
                       │  │ • Predice prezzi            │ │   │                 │       │              │
                       │  │ • Genera report             │ │   │                 │       │              │
                       │  └─────────────────────────────┘ │   │                 │       │              │
                       └────────────────────────────────-─┘   └─────────────────┘       └──────────────┘
</pre>

**Legenda:**
- <span style="color:#0074D9; font-weight:bold;">💻 <b>PC Locale</b></span>: punto di partenza, CSV originali
- <span style="color:#FF851B; font-weight:bold;">🛠️ <b>Airflow</b></span>: orchestratore ETL (bronze → silver → gold)
- <span style="color:#2ECC40; font-weight:bold;">☁️ <b>S3 bucket</b></span>: storage dati bronze/silver/gold
- <b>🥇*</b>: output gold usato come input per Databricks/ML
- <span style="color:#B10DC9; font-weight:bold;">🔮 <b>Databricks</b></span>: training e serving modello ML sui dati gold
---

## 1. Bronze Layer

**Ruolo:**  Carica i CSV grezzi in Parquet su Azure Blob Storage, aggiungendo la colonna `ingestion_date`. Nessuna pulizia, solo copia fedele.

**File:** `dags/bronze_etl_pipeline.py`
```python
def upload_parquet_to_blob(local_path, blob_name):
	df = pd.read_csv(local_path)
	df['ingestion_date'] = datetime.today().strftime('%Y-%m-%d')
	# ...scrittura su Azure Blob...
```
**Output:**
- `/bronze/YYYY-MM-DD/*.parquet` (tabelle grezze)

---

## 2. Silver Layer

**Ruolo:**  Applica regole di data quality e aggiunge colonne `dq_*` per ogni anomalia rilevata.

**File:** `dags/silver_etl_pipeline.py`
```python
def clean_bookings(df):
	# Corregge date invertite, importi negativi, valuta non valida
	# Aggiunge colonne dq_date_inverted, dq_amount_negative, dq_currency_invalid
	...
def add_dq_missing_flags(df):
	for col in df.columns:
		df[f'dq_missing_{col}'] = df[col].isnull()
```
**Output:**
- `/silver/YYYY-MM-DD/*.parquet` (tabelle pulite + flag DQ)

---

## 3. Gold Layer

**Ruolo:**  Calcola i KPI di business e produce tabelle pronte per analisi e ML.  Salva anche versioni pulite di bookings e rooms.

**File:** `dags/gold_etl_pipeline.py`
```python
# Esempio: calcolo KPI Daily Revenue
valid_bookings_df = bookings_df[...]
daily_revenue_df = valid_bookings_df.groupby('checkin_date').agg({
	'total_amount': 'sum', 'booking_id': 'count'
}).reset_index()
# Salvataggio parquet
push_parquet_to_blob(gold_container, f"{today}/daily_revenue.parquet", daily_revenue_df)
```
**Output:**
- `/gold/YYYY-MM-DD/daily_revenue.parquet`  
- `/gold/YYYY-MM-DD/bookings.parquet` (solo colonne utili, già pulite, vedi sotto)
- `/gold/YYYY-MM-DD/rooms.parquet` (senza colonne dq_)

---

## 4. Orchestrazione Airflow

**Ruolo:**  Coordina la pipeline ETL con un DAG sequenziale.

**File:** `dags/pipeline.py`
```python
with DAG(...) as dag:
	bronze = PythonOperator(task_id="bronze_etl", python_callable=bronze_etl_task)
	silver = PythonOperator(task_id="silver_etl", python_callable=silver_etl_task)
	gold = PythonOperator(task_id="gold_etl", python_callable=gold_etl_task)
	bronze >> silver >> gold
```

---

## 5. Output Gold per ML

**bookings.parquet** contiene solo le colonne:
```python
columns_needed = [
	"room_id", "dq_missing_customer_id", "dq_amount_negative", "source",
	"checkout_date", "booking_id", "dq_date_inverted", "total_amount",
	"checkin_date", "nights", "dq_currency_invalid", "customer_id", "status"
]
```
**rooms.parquet**: tutte le colonne tranne quelle che iniziano per `dq_`.

---

## 6. Machine Learning (vedi `MLDOCS.md`)

**Esempio snippet:**
```python
# Merge bookings e rooms gold
df_ml = bookings.merge(rooms, on='room_id', how='left')
df_ml['nights'] = (pd.to_datetime(df_ml['checkout_date']) - pd.to_datetime(df_ml['checkin_date'])).dt.days
# ...feature engineering, training, predizione...
```


**Dashboard Finale:**
dashboard/index.html
---

## 7. Flusso completo

1. **ETL notturno**: bronze → silver → gold (KPI e dati puliti)
2. **ML e report**: carica gold, predici prezzi, genera dashboard

---


---