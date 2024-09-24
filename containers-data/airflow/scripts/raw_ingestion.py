import requests, json, time, sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

dt_now = sys.argv[1]
endpoint = sys.argv[2]
hr = datetime.now().strftime("%H:%M")
app_name = "raw_ingestion_" + str(endpoint)
if endpoint == "":
    endpoint = None
    app_name = "raw_ingestion"
print("[DEBUG] dt_now: " + str(dt_now))
print("[DEBUG] endpoint: " + str(endpoint))
print("[DEBUG] hr: " + str(hr))

spark = (SparkSession.builder
             .appName(app_name) # Name the app
             .config("hive.metastore.uris", "thrift://metastore:9083") # Set external Hive Metastore
             .config("hive.metastore.schema.verification", "false") # Prevent some errors
             .config("spark.sql.repl.eagerEval.enabled", True)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .enableHiveSupport()
             .getOrCreate())

def autenticar():
    # endpoint da API para autenticar
    api_url = "https://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=fa1ae741481d20625673b2020fdd07bcfdcf5d60f27d226a069812a94de3edd0"

    # Inicialize uma sessão do requests
    session = requests.Session()

    try:
        # Faz a requisição post para a API usando a sessão
        response = session.post(api_url)

        # Verifique se a requisição foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            # Extraia o conteúdo JSON (ou texto) da resposta
            data = response.json()  # ou response.text se for texto
          
            return session                
        else:
            print(f"Falha ao acessar a API. Status Code: {session.post(api_url).status_code}")      
            
    finally:
        # Feche a sessão após o uso
        session.close()

def callAPIGet(api_url, session):
    try:    
        # Verifique se a requisição foi bem-sucedida (código de status 200)
        if session.get(api_url).status_code == 200:
            # Extraia o conteúdo JSON (ou texto) da resposta
            data = session.get(api_url).json()  # ou response.text se for texto
            
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            
            return None                  
        else:
            print(f"Falha ao acessar a API. Status Code: {session.post(api_url).status_code}")      
            
    finally:
        # Feche a sessão após o uso
        session.close()

def obterPrevisaoParada(stop_id, session):
    while True:
        r = session.get(f"https://api.olhovivo.sptrans.com.br/v2.1/Previsao/Parada?codigoParada={stop_id}")

        if r.status_code == 200:
            return {"stop_id": stop_id, **r.json()}

        elif r.status_code == 429:
            time.sleep(60)
            
        else:
            return None

def obterLinhas(route_id, session):
    while True:
        r = session.get(f"https://api.olhovivo.sptrans.com.br/v2.1/Linha/Buscar?termosBusca={route_id}")

        if r.status_code == 200:
            return {"route_id": route_id, "hr": hr, "data": r.json()}

        elif r.status_code == 429:
            time.sleep(60)
            
        else:
            return None

def obterParadasLinha(linha_id, session):
    while True:
        r = session.get(f"https://api.olhovivo.sptrans.com.br/v2.1/Parada/BuscarParadasPorLinha?codigoLinha={linha_id}")

        if r.status_code == 200:
            return {"linha_id": linha_id, "hr": hr, "data": r.json()}

        elif r.status_code == 429:
            time.sleep(60)
            
        else:
            return None

corredor_schema = StructType([
    StructField("cc", IntegerType(), True),
    StructField("nc", StringType(), True)
])

empresa_schema = StructType([
    StructField("hr", StringType(), True),
    StructField("e", ArrayType(StructType([
        StructField("a", IntegerType(), True),
        StructField("e", ArrayType(StructType([
            StructField("a", IntegerType(), True),
            StructField("c", IntegerType(), True),
            StructField("n", StringType(), True)
        ])), True),
    ])), True)
])

posicao_schema = StructType([
    StructField("hr", StringType(), True),
    StructField("l", ArrayType(StructType([
        StructField("c", StringType(), True),
        StructField("cl", IntegerType(), True),
        StructField("sl", IntegerType(), True),
        StructField("lt0", StringType(), True),
        StructField("lt1", StringType(), True),
        StructField("qv", IntegerType(), True),
        StructField("vs", ArrayType(StructType([
            StructField("p", IntegerType(), True),
            StructField("a", BooleanType(), True),
            StructField("ta", StringType(), True),
            StructField("py", FloatType(), True),
            StructField("px", FloatType(), True)
        ])), True),
    ])), True)
])

previsao_schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("hr", StringType(), True),
    StructField("p", StructType([
        StructField("cp", LongType(), True),
        StructField("np", StringType(), True),
        StructField("py", FloatType(), True),
        StructField("px", FloatType(), True),
        StructField("l", ArrayType(StructType([
            StructField("c", StringType(), True),
            StructField("cl", IntegerType(), True),
            StructField("sl", IntegerType(), True),
            StructField("lt0", StringType(), True),
            StructField("lt1", StringType(), True),
            StructField("qv", IntegerType(), True),
            StructField("vs", ArrayType(StructType([
                StructField("p", StringType(), True),
                StructField("a", BooleanType(), True),
                StructField("ta", StringType(), True),
                StructField("py", FloatType(), True),
                StructField("px", FloatType(), True)
            ])), True),
        ])), True)
    ]), True)
])

linhas_schema_pre = StructType([
    StructField("route_id", StringType(), True),
    StructField("hr", StringType(), True),
    StructField("data", ArrayType(StructType([
        StructField("cl", StringType(), True),
        StructField("lc", StringType(), True),
        StructField("lt", StringType(), True),
        StructField("sl", StringType(), True),
        StructField("tl", StringType(), True),
        StructField("tp", StringType(), True),
        StructField("ts", StringType(), True)
    ])), True)
])

linhas_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("hr", StringType(), True),
    StructField("cl", StringType(), True),
    StructField("lc", StringType(), True),
    StructField("lt", StringType(), True),
    StructField("sl", StringType(), True),
    StructField("tl", StringType(), True),
    StructField("tp", StringType(), True),
    StructField("ts", StringType(), True)
])

paradas_schema_pre = StructType([
    StructField("linha_id", StringType(), True),
    StructField("hr", StringType(), True),
    StructField("data", ArrayType(StructType([
        StructField("cp", StringType(), True),
        StructField("ed", StringType(), True),
        StructField("np", StringType(), True),
        StructField("px", StringType(), True),
        StructField("py", StringType(), True)
    ])), True)
])

paradas_schema = StructType([
    StructField("linha_id", StringType(), True),
    StructField("hr", StringType(), True),
    StructField("cp", StringType(), True),
    StructField("ed", StringType(), True),
    StructField("np", StringType(), True),
    StructField("px", StringType(), True),
    StructField("py", StringType(), True)
])

session = autenticar()

if endpoint == "corredor" or endpoint is None:
    corredor = callAPIGet("https://api.olhovivo.sptrans.com.br/v2.1/Corredor", session)
    corredor_df = spark.createDataFrame(corredor, corredor_schema)
    corredor_df.write.mode("append").format("json").save(f"s3a://raw/olhovivo/corredor/dt={dt_now}/")

if endpoint == "empresa" or endpoint is None:
    empresa = callAPIGet("https://api.olhovivo.sptrans.com.br/v2.1/Empresa", session)
    empresa_df = spark.createDataFrame(empresa, empresa_schema)
    empresa_df.write.mode("append").format("json").save(f"s3a://raw/olhovivo/empresa/dt={dt_now}/")

if endpoint == "posicao" or endpoint is None:
    posicao = callAPIGet("https://api.olhovivo.sptrans.com.br/v2.1/Posicao", session)
    posicao_df = spark.createDataFrame(posicao, posicao_schema)
    posicao_df.write.mode("append").format("json").save(f"s3a://raw/olhovivo/posicao/dt={dt_now}/")

if endpoint == "linhas" or endpoint is None:
    linhas = spark.read.csv("s3a://raw/GTFS/linhas/*.txt", header=True)
    linhas_rdd = linhas.select("route_id").distinct().rdd
    linhas_rdd_data = linhas_rdd.map(lambda x: x.asDict()["route_id"]).map(lambda x: obterLinhas(x, session)).map(lambda x: Row(data=json.dumps(x)))
    linhas_df = spark.createDataFrame(linhas_rdd_data, StructType([StructField("data", StringType(), True)]))
    linhas_df.write.mode("overwrite").format("parquet").save("s3a://raw/olhovivo/linhas_unparsed/")  # Etapa intermediaria
    print("[DEBUG] paradas_df count is " + str(linhas_df.count()))
    linhas_df = spark.read.schema(StructType([StructField("data", StringType(), True)])).parquet("s3a://raw/olhovivo/linhas_unparsed/")
    linhas_parsed_df = linhas_df.select(from_json("data", linhas_schema_pre).alias("data")).select("data.*").select("route_id", "hr", explode("data").alias("data")).select("route_id", "hr", "data.*")
    linhas_parsed_df.write.mode("append").format("json").save(f"s3a://raw/olhovivo/linhas/dt={dt_now}/")

if endpoint == "paradas" or endpoint is None:
    linhas_df = spark.read.schema(linhas_schema).format("json").load(f"s3a://raw/olhovivo/linhas/dt={dt_now}/")
    linhas_rdd = linhas_df.select("cl").distinct().rdd
    paradas_rdd = linhas_rdd.map(lambda x: x.asDict()["cl"]).map(lambda x: obterParadasLinha(x, session)).map(lambda x: Row(data=json.dumps(x)))
    paradas_df = spark.createDataFrame(paradas_rdd, StructType([StructField("data", StringType(), True)]))
    paradas_df.write.mode("overwrite").format("parquet").save("s3a://raw/olhovivo/paradas_unparsed/")  # Etapa intermediaria
    print("[DEBUG] paradas_df count is " + str(paradas_df.count()))
    paradas_df = spark.read.schema(StructType([StructField("data", StringType(), True)])).parquet("s3a://raw/olhovivo/paradas_unparsed/")
    paradas_parsed_df = paradas_df.select(from_json("data", paradas_schema_pre).alias("data")).select("data.*").select("linha_id", "hr", explode("data").alias("data")).select("linha_id", "hr", "data.*")
    paradas_parsed_df.write.mode("append").format("json").save(f"s3a://raw/olhovivo/paradas/dt={dt_now}/")

if endpoint == "previsao" or endpoint is None:
    paradas = spark.read.csv("s3a://raw/GTFS/paradas/*.txt", header=True)
    paradas_rdd = paradas.filter("stop_desc is not null").select("stop_id").rdd
    previsao = paradas_rdd.map(lambda x: x.asDict()["stop_id"]).map(lambda x: obterPrevisaoParada(x, session)).map(lambda x: Row(data=json.dumps(x)))
    previsao_df = spark.createDataFrame(previsao, StructType([StructField("data", StringType(), True)]))
    previsao_df.write.mode("overwrite").format("parquet").save("s3a://raw/olhovivo/previsao_unparsed/")  # Etapa intermediaria
    print("[DEBUG] previsao_df count is " + str(previsao_df.count()))
    previsao_df = spark.read.schema(StructType([StructField("data", StringType(), True)])).parquet("s3a://raw/olhovivo/previsao_unparsed/")
    previsao_parsed_df = previsao_df.select(from_json("data", previsao_schema).alias("data")).select("data.*")
    previsao_parsed_df.write.mode("append").format("json").save(f"s3a://raw/olhovivo/previsao/dt={dt_now}/")

print("[DEBUG] Done.")
