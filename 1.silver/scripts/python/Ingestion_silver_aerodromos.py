# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
from credentials import read_ini
import sys


# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_aerodromos").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")


#====================================================================================================
# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()
input_path  = read_ini(data)["hdfs_path_input_aerodromos"]

# Define file read
if sys.argv[1] != "-":
    input_path = input_path + "/icao=" + sys.argv[1]
else:
    pass

#====================================================================================================
# Read the file parquet
df_aerodromos = spark.read.parquet(input_path)
    
#===================================================================================================
# Insert Into in table aerodromos using append mode and partition by icao
db = "silver"
table = "aerodromos"
df_aerodromos.write.mode("append").partitionBy("icao").insertInto(f"{db}.{table}")

#====================================================================================================
# Stop Spark Session
spark.stop()
