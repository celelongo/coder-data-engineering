from airflow.hooks.base_hook import BaseHook
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import sqlalchemy as sa
import random as rand

#### Funciones ####
def connect_to_db(conn_id):
    connection = BaseHook.get_connection(conn_id)
    conn_string = f'postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}'
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine

def insert_data(table, df, conn, engine):
    df.to_sql(table, con=engine, if_exists='append', index=False)

def create_table_if_not_exists(table_name, conn):
    schema = 'celelongo_coderhouse'

    if table_name == 'artists':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id_artista VARCHAR(25) PRIMARY KEY,
            nombre VARCHAR(50),
            seguidores INTEGER,
            fecha_ingesta TIMESTAMP)
        """
    elif table_name == 'albums':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id_album VARCHAR(25) PRIMARY KEY,
            nombre VARCHAR(100),
            lanzamiento TIMESTAMP,
            canciones INTEGER,
            id_artista VARCHAR(25),
            fecha_ingesta TIMESTAMP)
        """
    elif table_name == 'tracks':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id_cancion VARCHAR(25) PRIMARY KEY,
            nombre VARCHAR(100),
            duracion INTEGER,
            reproducciones INTEGER,
            listas INTEGER,
            id_album VARCHAR(25),
            id_artista VARCHAR(25),
            fecha_ingesta TIMESTAMP)
        """
    conn.execute(create_table_query)

def get_spotify_credentials(conn_id):
    connection = BaseHook.get_connection(conn_id)
    cid = connection.login
    cpwd = connection.password
    client_credentials_manager = SpotifyClientCredentials(cid, cpwd)
    return client_credentials_manager

def get_spotify_artist(ccm, id_artista):
    sp = spotipy.Spotify(client_credentials_manager=ccm)
    output = sp.artist(id_artista)
    return output

def get_spotify_albums(ccm, id_artista):
    sp = spotipy.Spotify(client_credentials_manager=ccm)
    output = sp.artist_albums(id_artista)
    return output

def get_spotify_tracks(ccm, id_album):
    sp = spotipy.Spotify(client_credentials_manager=ccm)
    output = sp.album_tracks(id_album)
    return output

def get_track_info(ccm, id_track):
    sp = spotipy.Spotify(client_credentials_manager=ccm)
    track_info = sp.track(id_track)
    play_count = track_info['popularity'] if 'popularity' in track_info else None
    return play_count