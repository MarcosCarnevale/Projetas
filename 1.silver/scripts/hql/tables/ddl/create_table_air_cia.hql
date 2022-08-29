--------------------------------------------------------------create table air_cia--------------------------------------------------------------
CREATE EXTERNAL TABLE air_cia(
  razao_social string,
  cnpj int,
  atividades_aéreas string,
  endereço_sede string,
  telefone array<string>,
  e-mail string,
  decisao_operacional string,
  data_decisao_operacional timestamp,
  validade_operacional timestamp,
  icao string,
  iata string,
  area array<string>
) 
PARTITIONED BY (ano_mes int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/data/silver/air_cia';

