import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError



# endpoint da API para autenticar
api_url = "https://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=fa1ae741481d20625673b2020fdd07bcfdcf5d60f27d226a069812a94de3edd0"


# Inicialize uma sessão do requests
session = requests.Session()

# endpoint da API para autenticar
api_url_ls = "https://api.olhovivo.sptrans.com.br/v2.1/Linha/BuscarLinhaSentido?termosBusca=1705&sentido=1"

try:
    # Faz a requisição post para a API usando a sessão
    response = session.post(api_url)

    # Verifique se a requisição foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        # Extraia o conteúdo JSON (ou texto) da resposta
        data = response.json()  # ou response.text se for texto
        print("Dados da API:", data)

        response = session.post(api_url)
        
        if response.status_code == 200:
            # Extraia o conteúdo JSON (ou texto) da resposta
            data = response.json()  # ou response.text se for texto
            print("Dados da API:", data)
            

    else:
        print(f"Falha ao acessar a API. Status Code: {response.status_code}")

        
finally:
    # Feche a sessão após o uso
    session.close()