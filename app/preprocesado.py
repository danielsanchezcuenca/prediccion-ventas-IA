import pandas as pd
import os
import numpy as np

# 📌 Obtener la ruta raíz del proyecto sin importar desde dónde se ejecute
BASE_DIR = os.path.abspath(os.path.dirname(__file__))  # Carpeta 'app'
BASE_DIR = os.path.dirname(BASE_DIR)  # Subimos a la raíz 'Proyecto1'

# 📂 Definir rutas usando las mismas variables
data_path = os.path.join(BASE_DIR, "data", "raw")

# 🔍 Verificar que la carpeta existe
if not os.path.exists(data_path):
    raise FileNotFoundError(f"❌ ERROR: No se encontró la carpeta {data_path}")

# 📌 Listar archivos para confirmar que encuentra los CSV
archivos = os.listdir(data_path)
print("Archivos en la carpeta raw:", archivos)

#HOLIDAYS_EVENTS
holiday_df = pd.read_csv(os.path.join(data_path, "holidays_events.csv"))

#CONTROL DE FECHAS HOLIDAY

holiday_df["date"] = pd.to_datetime(holiday_df["date"])

fecha_min = holiday_df["date"].min()
fecha_max = holiday_df["date"].max()
 
nulos_date_holidays = holiday_df["date"].isnull().sum()

print(f"Los valores nulos en holidays.date son: {nulos_date_holidays}")
print(f" HOLIDAY: RANGO DE FECHAS DE {fecha_min} a {fecha_max}")


holiday_df["date"] = pd.to_datetime(holiday_df["date"])  # Asegurar que es datetime
holiday_df["year"] = holiday_df["date"].dt.year
holiday_df["month"] = holiday_df["date"].dt.month
holiday_df["day"] = holiday_df["date"].dt.day
holiday_df["day_of_week"] = holiday_df["date"].dt.weekday  # 0 = Lunes, 6 = Domingo
holiday_df["week_of_year"] = holiday_df["date"].dt.isocalendar().week  # Semana del año

#LIMPIEZA DE LA COLUMNA TRANSFERRED:

# Filtrar solo los registros donde transferred == False (es decir, NO transferidos)
holiday_df_clean = holiday_df[holiday_df.transferred == False]

# Eliminar la columna transferred, ya que ahora es innecesaria
holiday_df_clean = holiday_df_clean.drop("transferred", axis=1)

# Reiniciar el índice tras la limpieza
holiday_df_clean = holiday_df_clean.reset_index(drop=True)

#COMPROBACIÓN DE QUE ES CORRECTA

datos_iniciales = holiday_df.shape[0]

datosSinTransferred= holiday_df[holiday_df["transferred"] != False].shape[0]


holiday_df_clean = holiday_df[holiday_df["transferred"] == False]

datos_postFiltrado = holiday_df_clean.shape[0]

print(f"Los datos iniciales son: {datos_iniciales} los que hemos quitado {datosSinTransferred} los que nos quedan {datos_postFiltrado}  ")

nulos_date_holidays_clean = holiday_df_clean["date"].isnull().sum()


if nulos_date_holidays_clean > 0:
    raise ValueError(f"❌ ERROR: Existen {nulos_date_holidays_clean} valores nulos en la columna 'date' de holiday_df")

duplicados_holidays= holiday_df_clean.duplicated(subset=["date", "type", "locale", "locale_name","description"], keep=False)

if duplicados_holidays.any():
    raise ValueError(f"❌ ERROR: Existen {duplicados_holidays} valores duplicados en la clave única de holiday_df")
#OIL 

# Cargar y mostrar las primeras filas del archivo oil.csv
oil_df = pd.read_csv(os.path.join(data_path, "oil.csv"))


oil_df["date"] = pd.to_datetime(oil_df["date"])

#HAY NULOS EN DATE?

nulos_date_oil = oil_df["date"].isnull().sum()

print(f"Los valores nulos en oil.date son: {nulos_date_oil}")


if nulos_date_oil > 0:
    raise ValueError(f"❌ ERROR: Existen {nulos_date_oil} valores nulos en la columna 'date' de oil_df")

duplicados_oil= oil_df.duplicated(subset=["date"])

if duplicados_oil.any():
    raise ValueError(f"❌ ERROR: Existen {duplicados_oil} valores duplicados en la clave única de oil")

oil_df = oil_df.fillna(-1)  # Reemplaza NaN por -1


# STORES
stores = pd.read_csv(os.path.join(data_path, "stores.csv"))

store_nbr = stores["store_nbr"].isnull().sum()

print(f"Los valores nulos en stores.store_nbr son: {store_nbr}")

if store_nbr > 0:
    raise ValueError(f"❌ ERROR: Existen {store_nbr} valores nulos en la columna 'store_nbr' de stores")

duplicados_store = stores.duplicated(subset=["store_nbr"])

if duplicados_store.any():
    raise ValueError(f"❌ ERROR: Existen {duplicados_store} valores duplicados en la clave única de store")

#Transactions

transactions = pd.read_csv(os.path.join(data_path, "transactions.csv"))

transactions["date"] = pd.to_datetime(transactions["date"])

transactions_storenbr = transactions["store_nbr"].isnull().sum()
transactions_date = transactions["date"].isnull().sum()

print(f"Los valores nulos en transactions.store_nbr son: {transactions_storenbr}")
print(f"Los valores nulos en transactions.date son: {transactions_date}")

if ((transactions_storenbr > 0) and (transactions_date > 0)):
    raise ValueError(f"❌ ERROR: Existen {transactions_storenbr} valores nulos en la columna 'store_nbr' de transactions y Existen {transactions_date} valores nulos en la columna 'date' de transactions")

duplicados_transactions= transactions.duplicated(subset=["store_nbr","date"])

if duplicados_transactions.any():
    raise ValueError(f"❌ ERROR: Existen {duplicados_transactions} valores duplicados en la clave única de transactions")

DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")

holiday_df_clean.to_csv(os.path.join(DATA_PROCESSED,"holiday_events.csv"),index=False)
oil_df.to_csv(os.path.join(DATA_PROCESSED,"oil.csv"),index=False)
stores.to_csv(os.path.join(DATA_PROCESSED,"stores.csv"),index=False)
transactions.to_csv(os.path.join(DATA_PROCESSED,"transactions.csv"),index=False)