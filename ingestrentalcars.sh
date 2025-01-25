rm -f /home/hadoop/landing/car_rental_data/*.*

wget -P /home/hadoop/landing/car_rental_data CarRentalData.csv "https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv"
wget -P /home/hadoop/landing/car_rental_data -O /home/hadoop/landing/car_rental_data/states.csv "https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"

/home/hadoop/hadoop/bin/hdfs dfs -mkdir -p /ingest/car_rental_data
/home/hadoop/hadoop/bin/hdfs dfs -rm /ingest/car_rental_data/*.*
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/car_rental_data/*.* /ingest/car_rental_data/


#Opcional: Verificamos que los archivos se hayan copiado correctamente.
hdfs dfs -ls /ingest/car_rental_data
