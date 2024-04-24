Celeste Longo. Coderhouse Data Engineering. Comisión: 60340
Notas:

Sobre las correcciones hechas en la entrega 1, agrego los comentarios correspondientes para la 2da entrega:

1) Recordá incluir la columna temporal de control de ingesta de datos en RedShift.
Les cambié los nombres a las columnas temporales de las 3 tablas por "fecha_ingesta" para que quede claro que es esa la referencia temporal del dato.

2) El código parece correcto, pero no pude validarlo. Te pido por favor si luego podés enviarme por privado las credenciales de la API para que pueda validar tu código.
El archivo 'config.ini' se comparte por privado.

3) Recordá, si es posible, incluir algunos features adicionales además del nombre del artista y la cantidad de seguidores.
La db consta de 3 tablas: artists, albums y tracks
Mi idea es incluir estadísticas sobre las canciones y discos también. En esta segunda entrega intenté sumé 2 campos en la tabla "tracks": reproducciones y listas, el campo de reproducciones no trae exactamente la cantidad de reproducciones hasta el momento pero trae una métrica que da spotify llamada "popularity", en el campo playlists intenté agregar la cantidad playlists en las que aparece la canción pero el script demora mucho en ejecutarse, por eso dejé el for comentado. Mi intención es ver si para la próxima entrega puedo sumar este campo agregando una tabla "playlists" y haciendo el recuento con un storepprocedure en la db, no llegué a hacerlo para esta entrega, por ahora completo ese valor con un número generado aleatoriamente.
Paso las queries de select de las 3 tablas para que las revisen. 

SELECT id_artista, nombre, seguidores, fecha_ingesta
FROM celelongo_coderhouse.artists
order by fecha_ingesta DESC;

SELECT id_album, nombre, lanzamiento, canciones, id_artista, fecha_ingesta
FROM celelongo_coderhouse.albums
order by fecha_ingesta DESC;

SELECT id_cancion, nombre, duracion, reproducciones, listas, id_album, id_artista, fecha_ingesta
FROM celelongo_coderhouse.tracks
order by fecha_ingesta DESC;

También indagué sobre esta API para sumar más info: https://app.soundcharts.com/app/market/ pero la versión gratuita sólo permite consultar por 2 artistas:
Sandbox dataset
Please be advised that this environment provides a free but limited dataset.
The following entities are fully accessible through this documentation
Artists
Artist	Uuid	Spotify ID
Billie Eilish	11e81bcc-9c1c-ce38-b96b-a0369fe50396	6qqNVTkY8uBg9cP3Jd7DAH
Tones & I	ca22091a-3c00-11e9-974f-549f35141000	2NjfBq1NflQcKSeiDooVjY

4) En principio, la API se actualiza con cierta frecuencia. Luego te pido si puedes confirmar esto, y que al menos cada 2/3 días lo hace.
Por ahora lo estoy ejecutando manualmente, aún no está croneado en ningún lado, pero la idea es que se ejecute al menos 1 o 2 veces por día.
