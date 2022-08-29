
# Validate parameters contain information else iNsert information in variable PARAMETER_DATE
if [ -z "$1" ]
then
        PARAMETER_DATE='-'
else
        PARAMETER_DATE=$1
fi

spark-submit \
--name 'silver_vra' \
--master yarn \
--deploy-mode cluster \
--py-files /home/1.silver/utils/credentials.py,/home/1.silver/conf/hdfs_path.ini \
--driver-memory 4g \
--executor-memory 4g \
--executor-cores 2 \
--num-executors 2 \
--queue root.silver \
/home/1.silver/Ingestion_silver_vra.py $PARAMETER_DATE