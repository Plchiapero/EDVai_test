ingest.shfrom pyspark.context import SparkContext
from pyspark.sql.sesion import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)



######leo el csv desde HDFS y lo cargo en un dataframe
df2022 = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")
df2021 = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
df = spark.read.option("header", "true").option("sep", ";").csv("/ingest/aeropuertos_detalle.csv")

#####df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv")

##Unimos las dos tablas con UNION
df_2122 = df2021.union(df2022)

##opcional: si queres ver la data que esta en el DF###
##df_2122.show(5)
##df.printSchema()

#####creamos una vista del DF
df_2122.createOrReplaceTempView("df_2122_v")

## chequear df.createOrReplaceTempView("tripdata_vista")

##Casteamos y verificamos los cambios con printSchema y show
from pyspark.sql.functions import to_date, col
df_2122_cast = df_2122.withColumn("fecha", to_date(col("fecha"), "dd/MM/yyyy")).withColumn("pasajeros", col("pasajeros").cast("int"))
##df_2122_cast.printSchema()
##df_2122_cast.show(10)
df_2 = df.select(df.local.alias("aeropuerto"), df.oaci.alias("oac"), df.iata, df.tipo, df.denominacion, df.coordenadas, df.latitud, df.longitud, df.elev.cast("float"), df.uom_elev, df.ref, df.distancia_ref.cast>##df_2.printSchema()
##df_2.show(10)

##Generamos una vista de este nuevo df
df_2122_cast.createOrReplaceTempView("df_2122_cast_v")
df_2.createOrReplaceTempView("df2_v")

##Casteamos de acuerdo a lo solicitado en el punto del Schema 1
df_cast_nombres = spark.sql("SELECT to_date(fecha, 'yyyy/MM/dd') AS date, CAST(`Hora UTC` AS STRING) AS hora_utc,\
                            CAST(`Clase de Vuelo (todos los vuelos)` AS STRING) AS clase_de_vuelo, CAST(`Clasificaci\u00f3n Vuelo` AS \
                            STRING) AS clasificacion_de_vuelo, CAST(`Tipo de Movimiento` AS STRING) AS tipo_de_movimiento, \
                            CAST(aeropuerto AS STRING) AS aeropuerto, CAST(`Origen / Destino` AS STRING) AS origen_destino, \
                            CAST(`Aerolinea Nombre` AS STRING) AS aerolinea_nombre, CAST(aeronave AS STRING) AS aeronave, \
                            COALESCE(CAST(pasajeros AS INTEGER), 0) AS pasajeros FROM df_2122_cast_v")

##Generamos una vista para usar en SQL
df_cast_nombres.createOrReplaceTempView("df_2122_cast_v_final")

##Chequeamos la vista generada usando SQL
##spark.sql("DESCRIBE df_2122_cast_v_final").show()
##spark.sql("DESCRIBE df2_v").show()

##Generamos la vista filtrada segun solicitud punto 3
df_2122_filter = spark.sql("select * from df_2122_cast_v_final where fecha >= '2021-01-01' and fecha <= '2022-06-30'")

##Verificamos
##df_2122_filter.show()

##Generamos una vista de este nuevo df
df_2122_filter.createOrReplaceTempView("df_2122_cast_final")



#####insertamos el DF filtrado en la tabla aeropuerto_tabla
hc.sql("insert into examen_final.aeropuerto_tabla select * from df_2122_cast_v_final")
hc.sql("insert into examen_final.aeropuerto_detalles_tabla select * from df2_v")


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)
  
