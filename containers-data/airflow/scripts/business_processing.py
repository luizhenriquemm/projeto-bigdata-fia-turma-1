import requests, json, time, sys
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable
 
dt_now = sys.argv[1]
print("[DEBUG] dt_now: " + str(dt_now))

spark = (SparkSession.builder
             .appName('raw_ingestion') # Name the app
             .config("hive.metastore.uris", "thrift://metastore:9083") # Set external Hive Metastore
             .config("hive.metastore.schema.verification", "false") # Prevent some errors
             .config("spark.sql.repl.eagerEval.enabled", True)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .enableHiveSupport()
             .getOrCreate())

# Posicao
df_posicao = spark.table("stage.posicao")

df_posicao_b = df_posicao.select(
    "Hora", "Linha.*"
).select(
    "Hora", "c", "cl", "lt0", "lt1", "qv", "sl", explode("vs").alias("vs")
).select(
    "Hora", "c", "cl", "lt0", "lt1", "qv", "sl","vs.*"
)

window_spec = Window.partitionBy("cl", "sl", "p").orderBy(col("Hora").desc())
df_posicao_tratada = df_posicao_b.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num") 

DeltaTable.createIfNotExists(spark) \
    .tableName("business.posicao")\
    .addColumns(df_posicao_tratada.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "business.posicao")

deltaTable.alias('destiny') \
    .merge(
        df_posicao_tratada.alias('source'),
        'source.cl = destiny.cl and source.sl = destiny.sl and source.p = destiny.p'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()
deltaTable.vacuum(168)


# Previsão
df_previsao = spark.table("stage.previsao")
df_previsao_b = df_previsao.select(
    "hr", "p.*"
).select(
    "hr", "cp", explode("l").alias("l"), "np", "px", "py"
).select(
    "hr", "cp", "l.*", "np", "px", "py"
).select(
    "hr", 'cp', 'c', 'cl', 'lt0', 'lt1', 'qv', 'sl', explode('vs').alias("vs"), 'np', 'px', 'py'
).select(
    "hr", 'cp', 'c', 'cl', 'lt0', 'lt1', 'qv', 'sl', col("vs.a").alias("vs_a"), col("vs.p").alias("vs_p"), col("vs.px").alias("vs_px"), col("vs.py").alias("vs_py"), col("vs.ta").alias("vs_ta"), 'np', 'px', 'py'
)

window_spec = Window.partitionBy("cp", "cl", "vs_p").orderBy(col("hr").desc())
df_previsao_tratado = df_previsao_b.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num") 

DeltaTable.createIfNotExists(spark) \
    .tableName("business.previsao") \
    .addColumns(df_previsao_tratado.schema) \
    .execute()

deltaTable = DeltaTable.forName(spark, "business.previsao")

deltaTable.alias('destiny') \
    .merge(
        df_previsao_tratado.alias('source'),
        'source.cp = destiny.cp and source.cl = destiny.cl and source.vs_p = destiny.vs_p'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()
deltaTable.vacuum(168)


# Paradas
df_paradas = spark.table("stage.paradas")

DeltaTable.createIfNotExists(spark) \
    .tableName("business.paradas") \
    .addColumns(df_paradas.schema) \
    .execute()

deltaTable = DeltaTable.forName(spark, "business.paradas")

deltaTable.alias('destiny') \
    .merge(
        df_paradas.alias('source'),
        'source.stop_id = destiny.stop_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()
deltaTable.vacuum(168)


# Paradas API
df_paradas_api = spark.table("stage.paradas_api")

DeltaTable.createIfNotExists(spark) \
    .tableName("business.paradas_api") \
    .addColumns(df_paradas_api.schema) \
    .execute()

deltaTable = DeltaTable.forName(spark, "business.paradas_api")

deltaTable.alias('destiny') \
    .merge(
        df_paradas_api.alias('source'),
        'source.cp = destiny.cp'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()
deltaTable.vacuum(168)


# Trips
df_trips = spark.table("stage.trips")

DeltaTable.createIfNotExists(spark) \
    .tableName("business.trips") \
    .addColumns(df_trips.schema) \
    .execute()

deltaTable = DeltaTable.forName(spark, "business.trips")

deltaTable.alias('destiny') \
    .merge(
        df_trips.alias('source'),
        'source.route_id = destiny.route_id and source.direction_id = destiny.direction_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()
deltaTable.vacuum(168)


# Linhas API
df_linhas_api = spark.table("stage.linhas_api")

DeltaTable.createIfNotExists(spark) \
    .tableName("business.linhas_api") \
    .addColumns(df_linhas_api.schema) \
    .execute()

deltaTable = DeltaTable.forName(spark, "business.linhas_api")

deltaTable.alias('destiny') \
    .merge(
        df_linhas_api.alias('source'),
        'source.cl = destiny.cl'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()
deltaTable.vacuum(168)