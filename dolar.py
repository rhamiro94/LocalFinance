import requests
from datetime import datetime, timedelta
import pandas as pd
import time
from alphacast import Alphacast
import io
import psycopg2

base_url = "https://api.alphacast.io/datasets/5288/data?apiKey=ak_QQ2QSn8NV4tCUBPaw824&%24select=95700%2C95701&$filter='Date' ge 2024-01-01&$format=json"

response = requests.get(base_url)
data = response.json()

print(data)

records = data.get('data', [])

# Convertir los datos en un DataFrame de pandas
df = pd.DataFrame(records)
print(df)



df['Date'] = pd.to_datetime(df['Date'])

# Renombrar las columnas del DataFrame
df.rename(columns={
    'Date': 'date',
    'country': 'country',
    'Dolar Oficial': 'dolar_oficial',
    'Dolar Mayorista': 'dolar_mayorista'
}, inplace=True)


parametros_bd = {
    "dbname": "dolar",         # Nombre de tu base de datos
    "user": "postgres",        # Usuario de PostgreSQL
    "password": "Camila1995.", # Contraseña de PostgreSQL
    "host": "localhost"        # Host donde está corriendo PostgreSQL
}
conexion = psycopg2.connect(**parametros_bd)
cursor = conexion.cursor()

# Asegurarse de que la tabla se creó correctamente
create_table_query = '''
CREATE TABLE IF NOT EXISTS dolar (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    country TEXT,
    dolar_oficial NUMERIC,
    dolar_mayorista NUMERIC
);
'''
try:
    cursor.execute(create_table_query)
    conexion.commit()
    print("Tabla 'dolar' creada o ya existente.")
except Exception as e:
    print("Error al crear la tabla:", e)
    conexion.rollback()

# Iterar sobre el DataFrame e insertar datos en la base de datos
for index, row in df.iterrows():
    try:
        insert_query = '''
        INSERT INTO dolar (date, country, dolar_oficial, dolar_mayorista)
        VALUES (%s, %s, %s, %s)
        '''
        # Asegurarse de convertir los valores adecuadamente
        fecha = row['date']
        pais = row['country']
        dolar_oficial = row['dolar_oficial']
        dolar_mayorista = row['dolar_mayorista']
        
        cursor.execute(insert_query, (fecha, pais, dolar_oficial, dolar_mayorista))
        conexion.commit()
        print(f"Fila {index} insertada correctamente.")
    except Exception as e:
        print(f"Error al insertar fila {index}: {e}")
        conexion.rollback()

# Cerrar conexión
cursor.close()
conexion.close()