# Spark Context
from pyspark.context import *
from pyspark import *
from pyspark.sql.functions import  *
from pyspark.sql.types import * 

# Python Context
from credentials import read_ini
import sys

# Spark Session
spark = SparkSession.builder.master("local").appName("Ingestion_bronze_vra").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Read the file ini
with open("hdfs_path.ini") as f:
    data = f.readlines()

dict_data = read_ini(data)

# Set input and output paths
input_path = dict_data["hdfs_path_input_vra"]

# Define file read
if sys.argv[1] != "-":
    input_path = input_path + "/ano_mes=" + sys.argv[1]
else:
    pass


# Read the file parquet
vra = spark.read.parquet(input_path)

# Normalize header to snake case
vra = (
        vra.
        withColumnRenamed("ICAOEmpresaAérea", "icao_empresa_aerea").
        withColumnRenamed("NúmeroVoo", "numero_voo").
        withColumnRenamed("CódigoAutorização", "codigo_autorizacao").
        withColumnRenamed("CódigoTipoLinha", "codigo_tipo_linha").
        withColumnRenamed("ICAOAeródromoOrigem", "icao_aerodromo_origem").
        withColumnRenamed("ICAOAeródromoDestino", "icao_aerodromo_destino").
        withColumnRenamed("PartidaPrevista", "partida_prevista").
        withColumnRenamed("PartidaReal", "partida_real").
        withColumnRenamed("ChegadaPrevista", "chegada_prevista").
        withColumnRenamed("ChegadaReal", "chegada_real").
        withColumnRenamed("SituaçãoVoo", "situacao_voo").
        withColumnRenamed("CódigoJustificativa", "codigo_justificativa")
    )

# Insert Into in table vra using append mode and partition by ano_mes
db = "silver"
table = "vra"
vra.write.mode("append").partitionBy("ano_mes").insertInto(f"{db}.{table}")

# Stop Spark Session
spark.stop()
