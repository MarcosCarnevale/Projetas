
# Validate parameters contain information else iNsert information in variable PARAMETER_DATE
if [ -z "$1" ]
then
        PARAMETER_DATE='-'
else
        PARAMETER_DATE=$1
fi

spark-submit \
--name 'bronze_vra' \
--master yarn \
--deploy-mode cluster \
--py-files /home/utils/credentials.py,/home/utils/schemas.py,/home/conf/hdfs_path.ini \
--driver-memory 4g \
--executor-memory 4g \
--executor-cores 2 \
--num-executors 2 \
--queue root.bronze \
/home/0.bronze/Ingestion_bronze_vra.py $PARAMETER_DATE