## Piano ML - Regressione Prezzo Prenotazione

### Step 1: Feature Engineering
Arricchisci bookings con informazioni utili per la regressione.
```python
# Merge bookings con customer_value
df_ml = bookings.merge(customer_value, on='customer_id', how='left')
# Feature temporali
df_ml['season'] = pd.to_datetime(df_ml['checkin_date']).dt.month
df_ml['is_weekend'] = pd.to_datetime(df_ml['checkin_date']).dt.dayofweek >= 5
# Calcola notti
df_ml['nights'] = (pd.to_datetime(df_ml['checkout_date']) - pd.to_datetime(df_ml['checkin_date'])).dt.days
```

### Step 2: Preparazione Dataset
Definisci features e target, codifica variabili categoriche.
```python
# Encoding variabili categoriche
df_ml['is_weekend_encoded'] = df_ml['is_weekend'].astype(int)
df_ml['source_encoded'] = df_ml['source'].astype('category').cat.codes

X = df_ml[['nights', 'avg_ticket', 'season', 'is_weekend_encoded', 'source_encoded']]
y = df_ml['total_amount']
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
```

### Step 3: Modello e Validazione
Addestra il modello e valuta le performance.
```python
from sklearn.ensemble import RandomForestRegressor
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
predictions = model.predict(X_test)

# Metriche
from sklearn.metrics import mean_absolute_error, r2_score
print(f"MAE: {mean_absolute_error(y_test, predictions)}")
print(f"R²: {r2_score(y_test, predictions)}")
```

### Step 4: Integrazione Predicted_Price
Aggiungi la predizione al dataset e calcola la varianza.
```python
df_ml['predicted_price'] = model.predict(X)
df_ml['price_variance'] = abs(df_ml['total_amount'] - df_ml['predicted_price'])
```

### Step 5: Report e Dashboard
- Grafici: revenue trend, cancellation rate per source
- Feature importance del modello
- Distribuzione errori predizione
- Raccomandazioni business basate su anomalie