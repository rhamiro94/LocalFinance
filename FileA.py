##Actualizando...
# Parámetros de la API
# Obtener la fecha actual
import requests
from datetime import datetime, timedelta
import pandas as pd
import time
# Parámetros de la API
base_url = "https://trading.mav-sa.com.ar/cgi-bin/wspd_cgi.sh/WService=wsbroker1/cpd-concertacion-csv_v8.r"
user_id = "avsawebservice"
password = "avsa1601"

# Obtener la fecha actual
fecha_actual = datetime.now()

# Formato de fecha para la API (DD/MM/AA)
fecha_formato_api = "%d/%m/%y"

# Crear una lista para almacenar los datos
datos = []

# Bucle para generar fechas desde el principio del año actual hasta la fecha actual
fecha_actual = fecha_actual.replace(hour=0, minute=0, second=0, microsecond=0)  # Ajustar a la medianoche
fecha = fecha_actual
while fecha <= fecha_actual:
    # Construir la fecha en formato de texto
    fecha_texto = fecha.strftime(fecha_formato_api)

    # Construir la URL de la solicitud con la fecha actual
    url = f"{base_url}?mode=ws&an={user_id}&id=avsawebservice&password={password}&fecha=03/06/24"

    # Realizar la solicitud HTTP
    requests.packages.urllib3.disable_warnings()  # Desactivar la advertencia sobre solicitud HTTPS no verificada
    response = requests.get(url, verify=False)

    # Procesar la respuesta y almacenar los datos en la lista
    datos.append([fecha_texto] + response.text.strip().split(','))

    # Avanzar al siguiente día
    fecha += timedelta(days=1)

    # Esperar 60 segundos antes de la próxima ejecución
    time.sleep(60)

# Ahora puedes trabajar directamente con la lista de datos para realizar transformaciones
# Por ejemplo, puedes convertir la lista de datos en un DataFrame de pandas y aplicar transformaciones
df = pd.DataFrame(datos)
# Realizar transformaciones adicionales en el DataFrame si es necesario
print(df)

import csv

fecha_hoy = datetime.now().strftime("%Y-%m-%d")

# Generar el nombre del archivo CSV con la fecha de hoy
nombre_archivo = f"datos_{fecha_hoy}.csv"

# Escribir los datos en el archivo CSV
with open(nombre_archivo, "w", newline="", encoding="utf-8") as archivo:
    escritor_csv = csv.writer(archivo)
    escritor_csv.writerows(datos)

# Leer los datos del archivo CSV y crear un DataFrame
df = pd.read_csv(nombre_archivo, delimiter=';', quotechar='"', lineterminator='\n')

# Mostrar el DataFrame
print(df)

df = df.astype(str)

# Función para extraer la fecha y otros valores de la primera columna
def separar_fecha_valor(row):
    valores = row[0].split(',')
    fecha = valores[0].strip()
    otros_valores = ','.join(valores[2:]).strip()
    return pd.Series([fecha, otros_valores])

# Aplicar la función a cada fila del DataFrame y asignar los resultados a nuevas columnas
df[['Fecha', 'Subasta']] = df.apply(separar_fecha_valor, axis=1)

# Mostrar el DataFrame resultante
print(df)

# Función para extraer la fecha y otros valores de la primera columna
def separar_fecha_valor(row):
    valores = row[0].split(',')
    fecha = valores[0].strip()
    otros_valores = ','.join(valores[2:]).strip()
    return pd.Series([fecha, otros_valores])

# Aplicar la función a cada fila del DataFrame y asignar los resultados a nuevas columnas
df[['Fecha', 'Subasta']] = df.apply(separar_fecha_valor, axis=1)

# Mostrar el DataFrame resultante
print(df)

# Supongamos que tienes un DataFrame llamado df con una columna 'fecha'

# Define una función para procesar los valores de la columna 'fecha'
def procesar_fecha(valor):
    if "/" in str(valor):
        return valor
    else:
        return None  # O cualquier otro valor que desees poner en la columna 'otros_valores'

# Aplica la función a la columna 'fecha' y guarda los resultados en una nueva columna 'fecha_procesada'
df['Fecha'] = df['Fecha'].apply(procesar_fecha)

# Crea una nueva columna 'otros_valores' con los valores que no contenían '/'
df['Subasta'] = df['Fecha'].apply(lambda x: x if "/" not in str(x) else None)


df['Fecha'] = df['Fecha'].fillna(df[' Concertación'])

primera_columna_duplicada = df.iloc[:, 0]

# Crear la columna 'otros_valores'
df['Subasta'] = primera_columna_duplicada.apply(lambda x: x if '/' not in str(x) else None)

# Eliminar las dos primeras columnas
df = df.drop(df.columns[[0]], axis=1)
df = df.drop_duplicates()

# Obtener una lista con el nombre de todas las columnas
columnas = df.columns.tolist()

# Reorganizar la lista de columnas para que "Fecha" y "Otros_Valores" estén al principio
columnas = ['Fecha', 'Subasta'] + [col for col in columnas if col not in ['Fecha', 'Subasta']]

# Reorganizar el DataFrame con las nuevas columnas
df = df[columnas]


from datetime import datetime

# Convertir 'Fecha' a tipo datetime con el formato adecuado
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')

# Convertir 'Vto.' a tipo datetime con el formato adecuado
df['Vto.'] = pd.to_datetime(df['Vto.'], format='%d/%m/%Y')
df['dias_entre_fechas'] = (df['Vto.'] - df['Fecha']).dt.days

# Eliminar las comas de las cadenas y convertirlas a números de punto flotante
df[' Monto'] = df[' Monto'].str.split(',').str[0]
# Define una función para asignar la categoría en función de las palabras clave
def asignar_categoria(texto):
    if 'ARP' in texto:
        return 'ARPY'
    elif 'GAR' in texto:
        return 'GAR'
    elif 'MAV' in texto:
        return 'MAV'
    elif 'ACE' in texto:
        return 'ACEN'
    elif 'FDE' in texto:
        return 'FIDEM'
    elif 'NOV' in texto:
        return 'NOV'
    elif 'CRE' in texto:
        return 'CREC'
    elif 'POT' in texto:
        return 'POT'
    elif 'ACI' in texto:
        return 'ACIN'
    elif 'POT' in texto:
        return 'POT'
    elif 'BIN' in texto:
        return 'BIND'
    elif '#UGA' in texto:
        return 'GAR'
    elif '#UFA' in texto:
        return 'AVFE'
    elif '#UAR' in texto:
        return 'ARP'
    elif '#UAM' in texto:
        return 'AMER' 
    elif '#UAC' in texto:
        return 'ACIN'
    elif 'FED' in texto:
        return 'FEDE'
    elif 'FINT' in texto:
        return 'FINT'
    elif 'FTR' in texto:
        return 'AVFE'
    
    
    # AgregaMOS más condiciones según sea necesario
    else:
        return 'otra_categoria'

# Aplica la función a la columna 'texto' para crear la nueva columna 'SGR'
df['SGR'] = df[' Cod.Ch.'].apply(asignar_categoria)

# Muestra el DataFrame con la nueva columna
print(df)


df[' Tasa'] = df[' Tasa'].str.replace('"', '')

# Reemplazar la coma por un punto en los valores de la columna "tasa"
df[' Tasa'] = df[' Tasa'].str.replace(',', '.')

# Convertir la columna "tasa" a tipo float
df[' Tasa'] = df[' Tasa'].astype(float)

# Redondear los valores en la columna "tasa" al entero más cercano
df[' Tasa'] = df[' Tasa'].round()

columnas_a_ignorar = ['Fecha', 'Vto.',' Hora Conc.',' Cod.Ch.','SGR',' Moneda Liquidacion','Nombre Banco','Razón Librador/Deudor\r','Cond. Pyme', ' Banco',' C-V',' Hora Ord.',' Hora Conc.', ' Concertación', ' Liquidación', ' Reg.Oper.',' Cond.',' Banco',' Acreditación']



for columna in df.columns:
    if columna not in columnas_a_ignorar:
        # Verifica si los valores de la columna son numéricos
        if pd.api.types.is_numeric_dtype(df[columna]):
            # Convierte la columna a tipo de datos Int64
            df[columna] = df[columna].astype('Int64')

# Muestra el DataFrame con las columnas convertidas
print(df)


df[' Monto'] = df[' Monto'].astype(int)

df['duplicated'] = df.duplicated(subset=[' Reg.Oper.','Subasta',' C-V'], keep=False)

# Filtrar filas donde "Subasta" y "C-V" se repiten
df = df[~df['duplicated']]

# Eliminar la columna temporal
df = df.drop(columns=['duplicated'])

def asignar_periodo(dias_entre_fechas):
    if dias_entre_fechas < 60:
        return '0 a 60'
    elif 60 <= dias_entre_fechas < 90:
        return '60 a 90'
    elif 90 <= dias_entre_fechas < 120:
        return '90 a 120'
    elif 120 <= dias_entre_fechas < 180:
        return '120 a 180'
    elif 180 <= dias_entre_fechas < 210:
        return '180 a 210'
    elif 210 <= dias_entre_fechas < 240:
        return '210 a 240'
    elif 240 <= dias_entre_fechas < 300:
        return '240 a 300'
    else:
        return 'Más de 300'

# Aplicaa mosla función a la columna 'dias_entre_fechas' para crear la nueva columna 'periodo'
df['Periodo'] = df['dias_entre_fechas'].apply(asignar_periodo)


nuevo_orden = ['Fecha', 'Subasta',' Moneda' ,' C-V', ' Tasa',
       ' Contraparte', ' Hora Conc.', ' Concertación', ' Liquidación',
       ' Reg.Oper.', ' Id.Cheque', ' Banco', ' Nro.Cheque', ' Acreditación',
       ' Monto', ' Descuento', ' Der.Mer.', ' Der.Bol.', ' Comitente',
       ' CUIT Comprador', ' Hora Ord.', ' Warrant', ' Cond.', ' Cod.Ch.',
       ' Moneda Liquidacion', ' Tipo de Cambio', ' Sin Recurso',
       ' No a la Orden', 'Vto.', 'Nombre Banco', 'N. Sucursal', 'Pyme',
       'Primera.Neg', 'Tipo Instrumento', 'Custodio/Registro', 'Echeqid',
       'Plaza Cheque', 'Nro.Cta.Libr.', 'IVA Der.Mer.', 'CUIT Librador/Deudor',
       'CUIT PyME', 'Liquidador Compra', 'Caracter', 'CUIT Benef.',
       'Razón Benef.', 'Cond. Pyme', 'Razón Librador/Deudor\r',
       'dias_entre_fechas', 'SGR',' Segmento','Periodo']  # Define el nuevo orden de las columnas

# Reordena el DataFrame según el nuevo orden de columnas
df = df[nuevo_orden]

df.rename(columns={' Segmento': 'Segmento'}, inplace=True)

from sqlalchemy import create_engine

# Especificamos los detalles de la conexión a la base de datos PostgreSQL
usuario = 'postgres'
contraseña = 'Camila1995.'
host = 'localhost'
puerto = '5432'
base_de_datos = 'mav2024'

# Creamos la URL de conexión a la base de datos PostgreSQL
url_de_conexion = f'postgresql://{usuario}:{contraseña}@{host}:{puerto}/{base_de_datos}'

# Creamos una instancia del motor (engine) de SQLAlchemy
engine = create_engine(url_de_conexion)

# Guardamos el nuevo DataFrame en la tabla existente en la base de datos
nombre_tabla = 'mav2024'  # Especifica el nombre de la tabla donde deseas agregar los datos
df.to_sql(nombre_tabla, engine, if_exists='append', index=False)