Comandos para buildear la imagen:
docker build . --tag extending_airflow:latest
docker-compose up

Configuraciones de airflow:
Admin -> Connections:
[redshift]
Connection Id = redshift
Connection Type = Amazon Redshift
Host = data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
Database = data-engineer-database
User = celelongo_coderhouse
Password = CmJz098JpS
Port = 5439

[spotify]
Connection Id = spotify
Connection Type = HTTP
Host = https://api.spotify.com/v1/
Login = 7ac38a20661744608f548705dc42616a
Password = 3fc59f85995348a29949f58d5a863348
Port = 5439

Admin -> Variables:
Key: EMAIL_FROM; Val: celelongo@gmail.com
Key: EMAIL_TO; Val: celestelongo@hotmail.com
Key: password; Val: