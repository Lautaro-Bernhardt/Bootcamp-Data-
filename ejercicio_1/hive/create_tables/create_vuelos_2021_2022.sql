CREATE TABLE vuelos_2021_2022 (
  fecha DATE,
  horaUTC STRING,
  clase_de_vuelo STRING,
  clasificacion_de_vuelo STRING,
  tipo_de_movimiento STRING,
  aeropuerto STRING,
  origen_destino STRING,
  aerolinea_nombre STRING,
  aeronave STRING,
  pasajeros INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
