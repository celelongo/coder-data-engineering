import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from datetime import datetime

#### Functions ####
def build_conn_string(config_path, config_section):
    parser = ConfigParser()
    parser.read(config_path)

    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string 

def connect_to_db(conn_string):
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine

def insert_data(table, df):
    conn_str = build_conn_string('config.ini', 'redshift')
    conn, engine = connect_to_db(conn_str)
    schema = 'celelongo_coderhouse'
    
    #df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df.to_sql(table, con=engine, if_exists='append', index=False)
    conn.close()
    
def create_table_if_not_exists(table_name):
    conn_str = build_conn_string('config.ini', 'redshift')
    conn, engine = connect_to_db(conn_str)
    schema = 'celelongo_coderhouse'

    if table_name == 'artists':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id_artista VARCHAR(25) PRIMARY KEY,
            nombre VARCHAR(50),
            seguidores INTEGER,
            fecha DATETIME)
        """
        conn.execute(create_table_query)
    elif table_name == 'albums':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id_album VARCHAR(25) PRIMARY KEY,
            nombre VARCHAR(100),
            lanzamiento DATETIME,
            canciones INTEGER,
            id_artista VARCHAR(25),
            fecha DATETIME)
        """
        conn.execute(create_table_query)
    elif table_name == 'tracks':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id_cancion VARCHAR(25) PRIMARY KEY,
            nombre VARCHAR(100),
            duracion INTEGER,
            id_album VARCHAR(25),
            id_artista VARCHAR(25),
            fecha DATETIME)
        """
        conn.execute(create_table_query)
    conn.close()

def get_spotify_artist(cid, cpwd, id_artista):
    client_credentials_manager = SpotifyClientCredentials(cid, cpwd)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    output = sp.artist(id_artista)
    return(output)

def get_spotify_albums(cid, cpwd, id_artista):
    client_credentials_manager = SpotifyClientCredentials(cid, cpwd)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    output = sp.artist_albums(id_artista)
    return(output)

def get_spotify_tracks(cid, cpwd, id_album):
    client_credentials_manager = SpotifyClientCredentials(cid, cpwd)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    output = sp.album_tracks(id_album)
    return(output)

#### Main Code ####

config = ConfigParser()
config.read('config.ini')

cid = config['spotify']['clientid']
cpwd = config['spotify']['clientsecret']

artist_ids = ['0UAAJKwQZz8jVDoVtly8NA', # Massacre https://open.spotify.com/intl-es/artist/0UAAJKwQZz8jVDoVtly8NA?si=-q3GlBF_RbiSbDfnLtGZOA
            '5TDY6pHeoTA1l495jq6ohc', # Nunca Opuestos https://open.spotify.com/artist/5TDY6pHeoTA1l495jq6ohc?si=KmmFctXKSpaOH7FlTwi1jw
            '3NfKgcV9EaBHw31gDpshGf', # Perro Suizo https://open.spotify.com/artist/3NfKgcV9EaBHw31gDpshGf?si=VQlZsnQ4SBqyilH9QVrovg
            '2K9nbaNo6gGVMKmYjYS5um', # Cuyoman https://open.spotify.com/artist/2K9nbaNo6gGVMKmYjYS5um?si=nTxNCbf4SkaWQ29zMz19tQ
            '7vK1XnlXx9y8yfAmClQxqn', # Don Diego https://open.spotify.com/artist/7vK1XnlXx9y8yfAmClQxqn?si=oop632ikQ9WCUplO9BYFYg
            '3jXPyUbC7BboYHnbx3Qfhq', # Pablo Puntoriero https://open.spotify.com/artist/3jXPyUbC7BboYHnbx3Qfhq?si=uUjMfBWVSTGhqhiX_wHLMA
            '38io9QhP80YOC0hUMXYOHR', # Francheros https://open.spotify.com/artist/38io9QhP80YOC0hUMXYOHR?si=Lbxd0OSMT7iyJs9K0K79QQ
            '2XqYH6VooLrx5bjmPl7mLD', # Tranquilo Venancio https://open.spotify.com/artist/2XqYH6VooLrx5bjmPl7mLD?si=C3sUojDtQByLO4LWDvqxjA
            '3sVVMPMbALoko1Iub9ADj7'  # La perra que los parió https://open.spotify.com/artist/3sVVMPMbALoko1Iub9ADj7?si=2YWOCCduTvmji9VvYy3rpw
           ]

artist_list = []
for artist_id in artist_ids:  # Cambiar el nombre de la variable de artist a artist_id
    band = get_spotify_artist(cid, cpwd, artist_id)
    dict_band = {'artista': band['name'], 'id_artista': artist_id, 'seguidores': band['followers']['total'], 'discos': []}
    albums = get_spotify_albums(cid, cpwd, artist_id)
    album_list = albums['items']
    artist_list.append(dict_band)  # Agregar el dict_band a artist_list, no artist_ids
    for album in album_list:
        dict_album = {'nombre': album['name'], 'lanzamiento': album['release_date'], 'cant_canciones': album['total_tracks'], 'id': album['id'], 'canciones': []}
        dict_band['discos'].append(dict_album)
        tracks = get_spotify_tracks(cid, cpwd, album['id'])
        tracks_list = tracks['items']
        for track in tracks_list:
            dict_track = {'nombre': track['name'], 'duration': track['duration_ms'], 'id': track['id']}
            dict_album['canciones'].append(dict_track)

artistas_data = []
discos_data = []
canciones_data = []

for artista in artist_list:
    artista_data = {
        'id_artista': artista['id_artista'],
        'nombre': artista['artista'],
        'seguidores': artista['seguidores'],
        'fecha': datetime.now()
    }
    artistas_data.append(artista_data)

    for disco in artista['discos']:
        disco_data = {
            'id_album': disco['id'],
            'nombre': disco['nombre'],
            'lanzamiento': disco['lanzamiento'],
            'canciones': disco['cant_canciones'],
            'id_artista': artista['id_artista'],
            'fecha': datetime.now()
        }
        discos_data.append(disco_data)
        
        for cancion in disco['canciones']:
            segundos = int(cancion['duration'])/1000
            cancion_data = {
                'id_cancion': cancion['id'],
                'nombre': cancion['nombre'],
                'duracion': segundos,
                'id_album': disco['id'],
                'id_artista': disco['id'],
                'fecha': datetime.now()
            }
            canciones_data.append(cancion_data)

artistas_df = pd.DataFrame(artistas_data)
create_table_if_not_exists('artists')
insert_data('artists', artistas_df)
discos_df = pd.DataFrame(discos_data)
discos_df['lanzamiento'] = pd.to_datetime(discos_df['lanzamiento'], errors='coerce')
default_date = datetime.strptime('01-01-01 00:00:00', '%d-%m-%y %H:%M:%S')
discos_df['lanzamiento'] = discos_df['lanzamiento'].fillna(default_date)
insert_data('albums', discos_df)
create_table_if_not_exists('albums')
insert_data('albums', discos_df)
canciones_df = pd.DataFrame(canciones_data)
create_table_if_not_exists('tracks')
insert_data('tracks', canciones_df)
