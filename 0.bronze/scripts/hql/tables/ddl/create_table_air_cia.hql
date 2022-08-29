--------------------------------------------------------------create table air_cia--------------------------------------------------------------
CREATE EXTERNAL TABLE air_cia(
  razao_social string,
  cnpj string,
  atividades_aéreas string,
  endereço_sede string,
  telefone string,
  e-mail string,
  decisao_operacional string,
  data_decisao_operacional string,
  validade_operacional string,
  icao_iata string
) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/data/bronze/air_cia';

