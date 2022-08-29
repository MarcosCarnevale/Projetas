# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
from schemas import air_cia_schema
from credentials import read_ini
import sys

# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_air_cia").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()

dict_data = read_ini(data)

# Set input and output paths
input_path = dict_data["hdfs_path_input_air_cia"]

# Define file read 
if len(sys.argv) == 2:
    input_path = input_path + "/VRA_" + sys.argv[1] + ".json"
else:
    pass

#===================================================================================================
# Read the file csv
schema  = air_cia_schema()
air_cia = spark.read.csv(input_path, header=True, schema=schema, sep=";")

# Create a new column with the year and month of the date
air_cia = air_cia.withColumn("ano_mes", date_format(to_date(col("data_decisao_operacional"), "dd/MM/yyyy"), "yyyyMM"))

#===================================================================================================
# Insert Into in table air_cia using overwrite mode and partition by ano_mes
db = "bronze"
table = "air_cia"
air_cia.write.mode("overwrite").partitionBy("ano_mes").insertInto(f"{db}.{table}")

# Stop Spark Session
spark.stop()
