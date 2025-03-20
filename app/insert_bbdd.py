import pymysql
import pandas as pd
import os

# Conectar con MySQL
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="",  # Pon tu contraseña si la tienes
    database="store_sales"
)
cursor = conn.cursor()

# Definir ruta base y archivos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Subimos a la raíz 'Proyecto1'
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")

files = {
    "holiday_events": "holiday_events.csv",
    "transactions": "transactions.csv",
    "stores": "stores.csv",
    "oil": "oil.csv"
}

try:
    for table, filename in files.items():
        file_path = os.path.join(PROCESSED_PATH, filename)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ ERROR: No se encontró el archivo {filename} en {file_path}")

        df = pd.read_csv(file_path)  # Cargar CSV dinámicamente
        df = df.where(pd.notnull(df), None)  # Reemplazar NaN con None para evitar errores en MySQL

        # Construcción de la consulta según la tabla
        if table == "holiday_events":
            query = """
            INSERT IGNORE INTO holiday_events (date, type, locale, locale_name, description, transferred, year, month, day, day_week, week_year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = [(
                row["date"], row["type"], row["locale"], row["locale_name"], row["description"],
                row["transferred"], row["year"], row["month"], row["day"],
                row["day_of_week"], row["week_of_year"]
            ) for _, row in df.iterrows()]

        elif table == "transactions":
            query = """
            INSERT IGNORE INTO transactions (date, store_nbr, transactions)
            VALUES (%s, %s, %s)
            """
            data = [(row["date"], row["store_nbr"], row["transactions"]) for _, row in df.iterrows()]

        elif table == "stores":
            query = """
            INSERT IGNORE INTO stores (store_nbr, city, state, type, cluster)
            VALUES (%s, %s, %s, %s, %s)
            """
            data = [(row["store_nbr"], row["city"], row["state"], row["type"], row["cluster"]) for _, row in df.iterrows()]

        elif table == "oil":
            query = """
            INSERT IGNORE INTO oil (date, price)
            VALUES (%s, %s)
            """
            data = [(row["date"], row["dcoilwtico"]) for _, row in df.iterrows()]

        else:
            raise ValueError(f"❌ ERROR: No se reconoce la tabla {table}")

        # Iniciar transacción e insertar datos
        try:
            conn.begin()
            cursor.executemany(query, data)
            conn.commit()
            print(f"✅ Datos insertados correctamente en la tabla {table}.")
        except Exception as e:
            conn.rollback()
            raise ValueError(f"❌ ERROR al insertar en {table}: {e}")
            raise
except Exception as general_error:
    raise ValueError(f"❌ ERROR GENERAL: {general_error}")

finally:
    # Cerrar cursor y conexión para liberar recursos
    cursor.close()
    conn.close()
    print("🔌 Conexión a MySQL cerrada.")
