# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
import requests
from credentials import read_ini
from schemas import aerodromos_schema


# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_aerodromos").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")


#====================================================================================================
# Get icao code 
# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()

dict_data = read_ini(data)

# Set input and output paths
vra_input_path = dict_data["hdfs_path_output_vra"]
aerodromos_output_path = dict_data["hdfs_path_output_aerodromos"]

# Read the file parquet 
df_vra = spark.read.parquet(vra_input_path)
try:
    df_aerodromos = spark.read.parquet(aerodromos_output_path)
except:
    df_aerodromos = 0

# Get the icao code
icao_origin = df_vra.select("ICAOAeródromoOrigem").distinct().rdd.flatMap(lambda x: x).collect()
icao_destination = df_vra.select("ICAOAeródromoDestino").distinct().rdd.flatMap(lambda x: x).collect()

# Merge the icao code
icao = icao_origin + icao_destination
icao = list(set(icao))

#====================================================================================================
# Validate data in the table aerodromos
if df_aerodromos != 0:
    icao_aerodromos = df_aerodromos.select("icao").distinct().rdd.flatMap(lambda x: x).collect()
    icao_insert = list(set(icao) - set(icao_aerodromos))
else:
    icao_insert = icao

#====================================================================================================
# Read the file ini
with open("auth.ini") as f:
    data = f.readlines()
api_auth = read_ini(data)["X-RapidAPI-Key"]
api_host = read_ini(data)["X-RapidAPI-Host"]
api_url = read_ini(data)["api_url"]

# Get the data from the API
headers = {
    'x-rapidapi-key': api_auth,
    'x-rapidapi-host': api_host
    }

# Create the schema
schema = aerodromos_schema()


data = []
not_found = []
for i in icao_insert:
    querystring = {"icao":i}
    response = requests.request("GET", api_url, headers=headers, params=querystring)
    r = response.json()
    if len(r) > 1:
        data.append(r)
    else:
        not_found.append(i)
    
# Create the dataframes
df_aerodromos = spark.read.json(spark.sparkContext.parallelize([data]), schema=schema)
df_notfound = spark.createDataFrame(not_found, StringType())

#===================================================================================================
# Insert Into in table aerodromos using append mode and partition by icao
db = "bronze"
table = "aerodromos"
df_aerodromos.write.mode("append").partitionBy("icao").insertInto(f"{db}.{table}")

#===================================================================================================
# Create Column date_extracted in aerodromos_not_found
df_notfound = df_notfound.withColumn("date_extracted", current_date())

# Insert Into in table aerodromos_not_found using append mode
db = "bronze"
table = "aerodromos_not_found"
df_notfound.insertInto(f"{db}.{table}", append=True)

# Stop Spark Session
spark.stop()
