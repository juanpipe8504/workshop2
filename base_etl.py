import psycopg2
import pandas as pd
import re
import json

def traer_info():
    # Establecer la conexión a la base de datos y obtener los datos
    conn = psycopg2.connect(
        host='192.168.100.1',
        user='postgres',
        password='admin',
        database='etl_db'
    )
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM grammy')
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    conn.close()

    return df.to_json(orient='records')

def transformacion_grammy(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='read_db')
    json_data = json.loads(str_data)
    df =pd.json_normalize(data=json_data)
    # Eliminar filas con valores nulos en la columna 'artist'
   # grammy.dropna(subset=['artist'], inplace=True)

    # Función para eliminar coma
    def eliminar_coma(entrada):
        return re.sub(r',.*', '', entrada)

    # Función para eliminar punto
    def eliminar_punto(entrada):
        return re.sub(r';.*', '', entrada)

    # Aplicar eliminar_coma y eliminar_punto para el campo 'artist'
    df['artist'] = df['artist'].apply(eliminar_coma)
    df['artist'] = df['artist'].apply(eliminar_punto)

    # Eliminar columnas no deseadas
    columnas_a_eliminar = ['title', 'published_at', 'updated_at', 'img', 'workers']
    df= df.drop(columnas_a_eliminar, axis=1)

    return df.to_json(orient='records')