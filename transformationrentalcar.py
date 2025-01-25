from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CarRentalETL").enableHiveSupport().getOrCreate()

car_rental_df = spark.read.option("header", "true").option("sep", ",").csv("hdfs://172.17.0.2:9000/ingest/car_rental_data/CarRentalData.csv")
states_df = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/car_rental_data/states.csv")

# Transformaciones en car_rental_df
car_rental_df = car_rental_df.withColumnRenamed("fuelType", "fuel_type") \
    .withColumnRenamed("rating", "rating_float") \
    .withColumnRenamed("renterTripsTaken", "renter_trips_taken") \
    .withColumnRenamed("reviewCount", "review_count") \
    .withColumnRenamed("state", "state_abbrev")\
    .withColumnRenamed("location.city", "city")\
    .withColumnRenamed("location.country", "country")\
    .withColumnRenamed("location.latitude", "latitude")\
    .withColumnRenamed("location.longitude", "longitude")\
    .withColumnRenamed("location.state", "state")\
    .withColumnRenamed("owner.id", "owner_id")\
    .withColumnRenamed("rate.daily", "rate_daily")\
    .withColumnRenamed("vehicle.make", "make")\
    .withColumnRenamed("vehicle.model", "model")\
    .withColumnRenamed("vehicle.type", "type")\
    .withColumnRenamed("vehicle.year", "year")\
            .withColumn("rating", round(col("rating_float")).cast("int")) \
    .drop("rating_float")\
    .withColumn("fuel_type", lower(col("fuel_type")))

#Podemos chequear show y printSchema()
car_rental_df.show(10)
car_rental_df.printSchema()

# Transformaciones en states_df (renombrado de columnas para el join)

states_df = states_df.withColumnRenamed("Geo Point", "geo_point") \
                   .withColumnRenamed("Geo Shape", "geo_shape") \
                   .withColumnRenamed("Year", "rent_year") \
                   .withColumnRenamed("Official Code State", "state_code") \
                   .withColumnRenamed("Official Name State", "state_name") \
                   .withColumnRenamed("Iso 3166-3 Area Code", "iso_code") \
                   .withColumnRenamed("type", "area_type") \
                   .withColumnRenamed("United States Postal Service state abbreviation", "state_abrev") \
                   .withColumnRenamed("State FIPS Code", "state_fips_code") \
                   .withColumnRenamed("State GNIS Code", "state_gnis_code")
  #.withColumnRenamed("Type", "type") \

##joined_df = joined_df.withColumnRenamed("vehicle_year", "year") \
##                   .withColumnRenamed("year", "rental_year")

#Podemos chequear show y printSchema()
states_df.show(10)
states_df.printSchema()

# Join usando la columna state_abbrev de car_rental_df y state_abrev de states_df
joined_df = car_rental_df.join(states_df, car_rental_df["state_abbrev"] == states_df["state_abrev"], "left")

#Podemos chequear show y printSchema()
joined_df.show(10)
joined_df.printSchema()

# Filtrado y selección de columnas
transformed_df = joined_df.filter(col("rating").isNotNull()) \
        .filter(col("state_name") != "Texas") \
        .filter(col("state_abbrev") != "TX") \
        .filter(col("state_abrev") != "TX")

#Podemos chequear show y printSchema()
transformed_df.show(10)
transformed_df.printSchema()

transformed_df = transformed_df.withColumn("renter_trips_taken", col("renter_trips_taken").cast("int")) \
                               .withColumn("review_count", col("review_count").cast("int")) \
                               .withColumn("owner_id", col("owner_id").cast("int")) \
                               .withColumn("rate_daily", col("rate_daily").cast("int")) \
                               .withColumn("year", col("year").cast("int"))

#Seleccionamos las columnas a enviar
transformed_df_select = transformed_df.select("fuel_type", "rating", "renter_trips_taken", "review_count",\
                                        "city", "state_name","owner_id", "rate_daily", "make", "model", "year")
transformed_df_select.show(10)
##Generamos una vista de este nuevo df
transformed_df_select.createOrReplaceTempView("transformed_final_df")

#Podemos chequear la vista generada show
spark.sql("describe transformed_final_df").show()

#Aca estoy chequeando cual es el mejor camino para enviar el archivo a HIVE.
# Se utiliza para enviar desde hadoop al hive, pero dejamos la otra para armar el script para enviarlo desde airflow
# spark.sql("insert into car_rental_db.car_rental_analytics select * from transformed_final_df")

#Insertamos el DF filtrado en la tabla car_rental_analytics
hc.sql("insert into car_rental_db.car_rental_analytics select * from transformed_final_df")


spark.stop()
