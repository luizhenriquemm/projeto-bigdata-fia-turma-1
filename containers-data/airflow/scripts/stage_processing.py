import requests, json, time, sys
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable

# To do:
#   - Adiconar condicao no whenMatchedUpdateAll para o dado antigo nao sobrepor o dado mais recente
 
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


# Linhas
df_linhas = spark.read.csv("s3a://raw/GTFS/linhas", header=True)

DeltaTable.createIfNotExists(spark) \
    .tableName("stage.linhas")\
    .addColumns(df_linhas.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "stage.linhas")

deltaTable.alias('destiny') \
    .merge(
        df_linhas.alias('source'),
        'source.route_id = destiny.route_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()


# Paradas
df_paradas = spark.read.csv("s3a://raw/GTFS/paradas", header=True)

DeltaTable.createIfNotExists(spark) \
    .tableName("stage.paradas")\
    .addColumns(df_paradas.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "stage.paradas")

deltaTable.alias('destiny') \
    .merge(
        df_paradas.alias('source'),
        'source.stop_id = destiny.stop_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()


# Trips
df_trips = spark.read.csv("s3a://raw/GTFS/trips",header=True)

DeltaTable.createIfNotExists(spark) \
    .tableName("stage.trips")\
    .addColumns(df_trips.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "stage.trips")

deltaTable.alias('destiny') \
    .merge(
        df_trips.alias('source'),
        'source.route_id = destiny.route_id and source.direction_id = destiny.direction_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()


# Corredores
df_corredor = spark.read.json(f"s3a://raw/olhovivo/corredor/dt={dt_now}/")
df_corredor = df_corredor.select(
                   col('cc').alias('Codigo_Parada') 
                   ,col('nc').alias('Nome_Parada'))
df_corredor = df_corredor.dropDuplicates(["Codigo_Parada", "Nome_Parada"])

DeltaTable.createIfNotExists(spark) \
    .tableName("stage.corredor")\
    .addColumns(df_corredor.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "stage.corredor")

deltaTable.alias('destiny') \
    .merge(
        df_corredor.alias('source'),
        'source.Codigo_Parada = destiny.Codigo_Parada'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()


# Empresas
df_empresa = spark.read.json(f"s3a://raw/olhovivo/empresa/dt={dt_now}/")
df_empresa = df_empresa.select(\
                   explode('e').alias('Empresa') \
                   ,col('hr').alias('Hora'))
window_spec = Window.partitionBy("Empresa.a").orderBy(col("Hora").desc())
df_empresa_tratado = df_empresa.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num")

DeltaTable.createIfNotExists(spark) \
    .tableName("stage.empresa")\
    .addColumns(df_empresa_tratado.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "stage.empresa")

deltaTable.alias('destiny') \
    .merge(
        df_empresa_tratado.alias('source'),
        'source.Empresa.e = destiny.Empresa.e'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()


# Posição
df_posicao = spark.read.json(f"s3a://raw/olhovivo/posicao/dt={dt_now}/")
df_posicao = df_posicao.select(
                    col('hr').alias('Hora')
                    ,explode('l').alias('Linha'))

window_spec = Window.partitionBy("Linha.cl").orderBy(col("Hora").desc())
df_posicao_tratado = df_posicao.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num")

DeltaTable.createIfNotExists(spark) \
    .tableName("stage.posicao")\
    .addColumns(df_posicao_tratado.schema)\
    .execute()

deltaTable = DeltaTable.forName(spark, "stage.posicao")

deltaTable.alias('destiny') \
    .merge(
        df_posicao_tratado.alias('source'),
        'source.Linha.cl = destiny.Linha.cl and source.Linha.sl = destiny.Linha.sl'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

deltaTable.optimize()


# Previsao
# df_previsao = spark.read.json(f"s3a://raw/olhovivo/previsao/dt={dt_now}/")