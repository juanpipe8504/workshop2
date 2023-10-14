import pandas as pd
import json

import psycopg2

def merge(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='transform_csv')
    json_data = json.loads(str_data)
    transform_csv =pd.json_normalize(data=json_data)

    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='transform_db')
    json_data = json.loads(str_data)
    transform_grammy =pd.json_normalize(data=json_data)

    union = transform_grammy.merge(transform_csv, how='inner',
                            left_on=['nominee', 'artist'],
                            right_on=['track_name', 'artists'])
    return union.to_json(orient='records')


config = {
    "user": "postgres",
    "password": "admin",
    "database": "etl_db"
}

def create_connection():
    try:
        cnx = psycopg2.connect(
            host='localhost',
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        print('Conexión exitosa!!')
    except psycopg2.Error as e:
        cnx = None
        print('No se puede conectar:', e)
    return cnx
def load(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='merge')
    json_data = json.loads(str_data)
    carga_csv =pd.json_normalize(data=json_data)

    cnx = None
    insert_query = """
    INSERT INTO merge (year, nominee, artist, winner, album_name, track_name, popularity, track_genre, duration_seconds)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cnx = create_connection()  # Assuming you have a function named create_connection to establish the DB connection
        cur = cnx.cursor()
        for index, row in union.iterrows():
            values = (
                row['year'], row['nominee'], row['artist'], row['winner'], row['album_name'],
                row['track_name'], row['popularity'], row['track_genre'], row['duration_seconds']
            )
            cur.execute(insert_query, values)
        cur.close()
        cnx.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()