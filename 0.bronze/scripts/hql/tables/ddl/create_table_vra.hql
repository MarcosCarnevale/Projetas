--------------------------------------------------------------create table vra--------------------------------------------------------------
CREATE EXTERNAL TABLE vra(
    icao_empresa_aerea string,
    numero_voo string,
    codigo_autorizacao string,
    codigo_tipo_linha string,
    icao_aerodromo_origem string,
    icao_aerodromo_destino string,
    partida_prevista string,
    partida_real string
    chegada_prevista string
    chegada_real string
    situacao_voo string,
    codigo_justificativa string
) 
PARTITIONED BY (ano_mes int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/data/bronze/vra';

