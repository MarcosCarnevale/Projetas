--------------------------------------------------------------create table aerodromos--------------------------------------------------------------
CREATE EXTERNAL TABLE aerodromos(
  id int,
  iata string,
  name string,
  location string,
  street_number string,
  street string,
  city string,
  county string,
  state string,
  country_iso string,
  country string,
  postal_code string,
  phone string,
  latitude string,
  longitude string,
  uct string,
  website string
) 
PARTITIONED BY (icao string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/data/silver/aerodromos';

