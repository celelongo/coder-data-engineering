from datetime import datetime, timedelta
import utils
import pandas as pd
import random as rand
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator

import smtplib
from email.mime.text import MIMEText

default_args = {
    'owner': 'CelesteLongo',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

#### Main ####
def main(**kwargs):
    print(f"Inicio: {datetime.now()}")

    # Tomo las credenciales de Spotify y Redshift de Airflow Connections
    spotify_conn_id = 'spotify'
    redshift_conn_id = 'redshift'

    ccm = utils.get_spotify_credentials(spotify_conn_id)

    artist_ids = ['0UAAJKwQZz8jVDoVtly8NA', '5TDY6pHeoTA1l495jq6ohc', '3NfKgcV9EaBHw31gDpshGf', 
                  '2K9nbaNo6gGVMKmYjYS5um', '7vK1XnlXx9y8yfAmClQxqn', '3jXPyUbC7BboYHnbx3Qfhq', 
                  '38io9QhP80YOC0hUMXYOHR', '2XqYH6VooLrx5bjmPl7mLD', '3sVVMPMbALoko1Iub9ADj7']    

    artist_list = []
    list_mail_artist = []
    list_mail_track_may_30 = []
    list_mail_track_0 = []
    for artist_id in artist_ids:
        band = utils.get_spotify_artist(ccm, artist_id)
        dict_band = {'artista': band['name'], 'id_artista': artist_id, 'seguidores': band['followers']['total'], 'discos': []}
        albums = utils.get_spotify_albums(ccm, artist_id)
        album_list = albums['items']
        artist_list.append(dict_band)
        if dict_band['seguidores']>15000:
            list_mail_artist.append(band['name'])
        for album in album_list:
            dict_album = {'nombre': album['name'], 'lanzamiento': album['release_date'], 'cant_canciones': album['total_tracks'], 'id': album['id'], 'canciones': []}
            dict_band['discos'].append(dict_album)
            tracks = utils.get_spotify_tracks(ccm, album['id'])
            tracks_list = tracks['items']
            for track in tracks_list:
                play_count = utils.get_track_info(ccm, track['id'])
                dict_track = {'nombre': track['name'], 'duracion': track['duration_ms'], 'reproducciones': play_count, 'listas': rand.randint(20, 500), 'id': track['id']}
                dict_album['canciones'].append(dict_track)
                if play_count>30:
                    list_mail_track_may_30.append(track['name'])
                elif play_count==0:
                    list_mail_track_0.append(track['name'])

    artistas_data = []
    discos_data = []
    canciones_data = []
    fecha_ejecucion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for artista in artist_list:
        sp_id_artista = artista['id_artista']
        artista_data = {
            'id_artista': sp_id_artista,
            'nombre': artista['artista'],
            'seguidores': artista['seguidores'],
            'fecha_ingesta': fecha_ejecucion
        }
        artistas_data.append(artista_data)

        for disco in artista['discos']:
            disco_data = {
                'id_album': disco['id'],
                'nombre': disco['nombre'],
                'lanzamiento': disco['lanzamiento'],
                'canciones': disco['cant_canciones'],
                'id_artista': sp_id_artista,
                'fecha_ingesta': fecha_ejecucion
            }
            discos_data.append(disco_data)

            for cancion in disco['canciones']:
                segundos = int(cancion['duracion']) / 1000
                cancion_data = {
                    'id_cancion': cancion['id'],
                    'nombre': cancion['nombre'],
                    'duracion': segundos,
                    'reproducciones': cancion['reproducciones'],
                    'listas': cancion['listas'],
                    'id_album': disco['id'],
                    'id_artista': sp_id_artista,
                    'fecha_ingesta': fecha_ejecucion
                }
                canciones_data.append(cancion_data)

    artistas_df = pd.DataFrame(artistas_data)
    con, eng = utils.connect_to_db(redshift_conn_id)
    utils.create_table_if_not_exists('artists', con)
    utils.insert_data('artists', artistas_df, con, eng)
    discos_df = pd.DataFrame(discos_data)
    discos_df['lanzamiento'] = pd.to_datetime(discos_df['lanzamiento'], errors='coerce')
    default_date = datetime.strptime('01-01-01 00:00:00', '%d-%m-%y %H:%M:%S')
    discos_df['lanzamiento'] = discos_df['lanzamiento'].fillna(default_date)
    utils.create_table_if_not_exists('albums', con)
    utils.insert_data('albums', discos_df,  con, eng)
    canciones_df = pd.DataFrame(canciones_data)
    utils.create_table_if_not_exists('tracks', con)
    utils.insert_data('tracks', canciones_df,  con, eng)
    con.close()
    return list_mail_artist, list_mail_track_0, list_mail_track_may_30
    print(f"Fin  : {datetime.now()}")

def send_email(**kwargs):
    ti = kwargs['ti']
    sender_email = Variable.get("EMAIL_FROM")
    recipient_email = Variable.get("EMAIL_TO")
    pwd = Variable.get("password")
    list_mail_artist, list_mail_track_0, list_mail_track_may_30 = ti.xcom_pull(task_ids='main')
    subject =  f"Resumen de tus artistas {datetime.today().strftime('%Y-%m-%d')}"
    body = f"""Hola!\n\n
        Resumen del día {datetime.today().strftime('%Y-%m-%d')} 
        \n\nLas siguientes bandas tienen más de 15000 seguidores: {list_mail_artist} 
        \n\nEstas canciones tienen más de 30 reproducciones: {list_mail_track_may_30}
        \n\nEstas canciones no tuvieron reproducciones: {list_mail_track_0}
        \n\nSaludos!
        \nCele."""
    msg = MIMEText(body, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, pwd)
            server.sendmail(sender_email, recipient_email, msg.as_string())
            print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")

with DAG(
    default_args=default_args,
    dag_id='CelesteLongo_EntregaFinal',
    description='Entrega Final',
    start_date=datetime(2024, 6, 5, 9),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    main_process = PythonOperator(
        task_id='main',
        python_callable=main,
        provide_context=True
    )
    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_email,
        provide_context=True
    )
    main_process >> send_email