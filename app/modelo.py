import pymysql
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import joblib
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="store_sales"
)

query = "select * FROM vistaglobal"

df = pd.read_sql(query, conn)

conn.close()

print("✅ Datos cargados desde la vista:")
print(f"\nTotal de filas: {len(df)}")
df['fecha'] = pd.to_datetime(df['fecha'])
df = df.sort_values('fecha').reset_index(drop=True)

for idx, row in df[df['precioGlobal'].isnull()].iterrows():
    fecha_actual = row['fecha']

    # 1️⃣ Buscar valor en la misma fecha
    mismo_dia = df[(df['fecha'] == fecha_actual) & (df['precioGlobal'].notnull())]
    if not mismo_dia.empty:
        df.at[idx, 'precioGlobal'] = mismo_dia['precioGlobal'].mean()
        continue

    # 2️⃣ Buscar anterior y posterior más cercanos (iterando hasta encontrar ambos)
    anterior = None
    posterior = None
    step = 1

    while anterior is None or posterior is None:
        # Buscar hacia atrás
        if anterior is None:
            fila_ant = df[(df['fecha'] < fecha_actual - pd.Timedelta(days=step)) & (df['precioGlobal'].notnull())].tail(1)
            if not fila_ant.empty:
                anterior = fila_ant['precioGlobal'].values[0]
        # Buscar hacia adelante
        if posterior is None:
            fila_post = df[(df['fecha'] > fecha_actual + pd.Timedelta(days=step)) & (df['precioGlobal'].notnull())].head(1)
            if not fila_post.empty:
                posterior = fila_post['precioGlobal'].values[0]
        step += 1

    # 3️⃣ Calcular la media entre anterior y posterior más cercanos
    df.at[idx, 'precioGlobal'] = (anterior + posterior) / 2

print("✅ Nulos tratados con lógica avanzada.")

#OUTLIERS
percentil_95 = df['transacciones'].quantile(0.99)
print(f"⚠️ Valor del percentil 95: {percentil_95}")

# Filtrar los outliers por encima del percentil 95
outliers = df[df['transacciones'] > percentil_95]
print(f"🔍 Número de outliers: {outliers.shape[0]}")

df = df[df['transacciones'] < percentil_95]

df["type"]= df["type"].apply(lambda x:1 if x == 'Holiday' else 0)
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score, KFold
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error
import numpy as np

# 1️⃣ Features y target
X = df.drop(columns=['transacciones', 'fecha'])
y = df['transacciones']

# 2️⃣ Encoding simple para categóricas
for col in X.select_dtypes(include='object').columns:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))

# 3️⃣ Crear Random Forest
rf_model = RandomForestRegressor(
    n_estimators=300,  # Número de árboles
    max_depth=10,      # Profundidad máxima
    min_samples_split=5,
    min_samples_leaf=4,
    random_state=42,
    n_jobs=-1
)

# 4️⃣ Validación cruzada
kfold = KFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(rf_model, X, y, cv=kfold, scoring='neg_root_mean_squared_error')

print(f"🚀 RMSE medio en validación cruzada: {-np.mean(cv_scores):.2f}")
print(f"📊 RMSEs individuales: {-cv_scores}")

# 5️⃣ Entrenamiento final con todo el dataset
rf_model.fit(X, y)
print("✅ Modelo Random Forest entrenado.")

# Ruta base desde el archivo actual
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Ruta absoluta a la carpeta models
models_dir = os.path.join(base_dir, 'models')

# Guardar modelo entrenado
joblib.dump(rf_model, os.path.join(models_dir, 'random_forest_model.pkl'))