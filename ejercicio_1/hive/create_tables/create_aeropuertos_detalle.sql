CREATE TABLE aeropuertos_detalle (
  aeropuerto STRING,
  oac STRING,
  iata STRING,
  tipo STRING,
  denominacion STRING,
  coordenadas STRING,
  latitud STRING,
  longitud STRING,
  elev FLOAT,
  uom_elev STRING,
  ref STRING,
  distancia_ref FLOAT,
  direccion_ref STRING,
  condicion STRING,
  control STRING,
  region STRING,
  uso STRING,
  trafico STRING,
  sna STRING,
  concesionado STRING,
  provincia STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
