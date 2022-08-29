spark-submit \
--name 'bronze_aerodromos' \
--master yarn \
--deploy-mode cluster \
--py-files /home/utils/credentials.py,/home/utils/schemas.py,/home/conf/auth.ini \
--driver-memory 4g \
--executor-memory 4g \
--executor-cores 2 \
--num-executors 2 \
--queue root.bronze \
/home/0.bronze/Ingestion_bronze_aerodromos.py