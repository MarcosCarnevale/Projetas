# Validate parameters contain information else insert information in variable PARAMETER_ICAO
if [ -z "$1" ]
then
        PARAMETER_ICAO='-'
else
        PARAMETER_ICAO=$1
fi

spark-submit \
--name 'silver_aerodromos' \
--master yarn \
--deploy-mode cluster \
--py-files /home/1.silver/utils/credentials.py,/home/1.silver/conf/auth.ini \
--driver-memory 4g \
--executor-memory 4g \
--executor-cores 2 \
--num-executors 2 \
--queue root.silver \
/home/1.silver/Ingestion_silver_aerodromos.py $PARAMETER_ICAO