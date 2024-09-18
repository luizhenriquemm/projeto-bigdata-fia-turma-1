import requests, json, time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('load').enableHiveSupport().getOrCreate()

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
    StructField("p", ArrayType(StructType([
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
                StructField("p", IntegerType(), True),
                StructField("a", BooleanType(), True),
                StructField("ta", StringType(), True),
                StructField("py", FloatType(), True),
                StructField("px", FloatType(), True)
            ])), True),
        ])), True)
    ])), True)
])

session = autenticar()

corredor = callAPIGet("https://api.olhovivo.sptrans.com.br/v2.1/Corredor", session)
corredor_df = spark.createDataFrame(corredor, corredor_schema)
corredor_df.write.mode("append").format("json").save("s3a://raw/olhovivo/corredor/")

empresa = callAPIGet("https://api.olhovivo.sptrans.com.br/v2.1/Empresa", session)
empresa_df = spark.createDataFrame(empresa, empresa_schema)
empresa_df.write.mode("append").format("json").save("s3a://raw/olhovivo/empresa/")

posicao = callAPIGet("https://api.olhovivo.sptrans.com.br/v2.1/Posicao", session)
posicao_df = spark.createDataFrame(posicao, posicao_schema)
posicao_df.write.mode("append").format("json").save("s3a://raw/olhovivo/Posicao/")

paradas = spark.read.csv("s3a://raw/GTFS/paradas/*.txt", header=True)
paradas_rdd = paradas.filter("stop_desc is not null").select("stop_id").rdd

previsao = paradas_rdd.map(lambda x: x.asDict()["stop_id"]).map(lambda x: obterPrevisaoParada(x, session)).map(lambda x: Row(val=json.dumps(x)))

previsao_df = spark.createDataFrame(previsao, previsao_schema)
previsao_df.show(truncate=False)