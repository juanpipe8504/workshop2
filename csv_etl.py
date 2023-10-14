import pandas as pd
import json
import logging
import re

def leer_csv():
    df = pd.read_csv("./api_dag/spotify_dataset.csv")
    df.dropna(inplace=True)
    return df.to_json(orient='records')

def transformacion(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='read_csv')
    json_data = json.loads(str_data)
    df =pd.json_normalize(data=json_data)

    def eliminar_coma(entrada):
        return re.sub(r',.*', '', entrada)

    
    def eliminar_punto(entrada):
        return re.sub(r';.*', '', entrada)

    #elimina lss coma y eliminar_punto para conservar solo el primer artista
    df['artists'] = df['artists'].apply(eliminar_coma)
    df['artists'] = df['artists'].apply(eliminar_punto)

    # Calcula la duración en segundos porque estaba en milisegundos
    df['duration_seconds'] = df['duration_ms'] / 1000

    # elimina columnas que no necesito
    columnas_a_eliminar = ['Unnamed: 0', 'explicit', 'danceability', 'energy', 'duration_ms', 'loudness',
                            'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence',
                            ]
    df = df.drop(columnas_a_eliminar, axis=1)

    return df.to_json(orient='records')