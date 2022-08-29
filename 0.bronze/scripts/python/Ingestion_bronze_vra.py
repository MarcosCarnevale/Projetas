# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
from schemas import vra_schema
from credentials import read_ini
import sys



# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_vra").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()

dict_data = read_ini(data)

# Set input paths
input_path = dict_data["hdfs_path_input_vra"]

# Define file read 
if sys.argv[1] != "-":
    input_path = input_path + "/VRA_" + sys.argv[1] + ".json"
else:
    pass

#===================================================================================================
# Read the file json
schema  = vra_schema()
vra = spark.read.json(input_path, schema=schema)

# Create a new column with the year and month of the date
vra = vra.withColumn("ano_mes", date_format(col("PartidaPrevista"), "yyyyMM"))

#===================================================================================================
# Insert Into in table vra using append mode and partition by ano_mes
db = "bronze"
table = "vra"
vra.write.mode("append").partitionBy("ano_mes").insertInto(f"{db}.{table}")

# Stop Spark Session
spark.stop()
