import requests
import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError

def autenticar():

    # endpoint da API para autenticar
    api_url = "https://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=fa1ae741481d20625673b2020fdd07bcfdcf5d60f27d226a069812a94de3edd0"


    # Inicialize uma sessão do requests
    session = requests.Session()

    try:
        # Faz a requisição post para a API usando a sessão
        response = session.post(api_url)

        # Verifique se a requisição foi bem-sucedida (código de status 200)
        if session.post(api_url).status_code == 200:
            # Extraia o conteúdo JSON (ou texto) da resposta
            data = session.post(api_url).json()  # ou response.text se for texto
          
            return session                
        else:
            print(f"Falha ao acessar a API. Status Code: {session.post(api_url).status_code}")      
            
    finally:
        # Feche a sessão após o uso
        session.close()


def save_file(content, filename):
    """Salva o conteúdo em um arquivo para inspeção."""
    with open(filename, 'wb') as f:
        f.write(content)

def validate_kmz_content(content):
    """Verifica se o conteúdo pode ser aberto como um arquivo ZIP (KMZ)."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as kmz:
            return True
    except zipfile.BadZipFile:
        return False


def callKmz(api_url, session):
    # Passo 1: Baixar o arquivo KMZ da API
    # Faz a requisição post para a API usando a sessão
    response = session.get(api_url)
    print(response.status_code)

    # Verificar se a resposta foi bem-sucedida
    if response.status_code == 200:
        kmz_data = response.content

        # Salvar o conteúdo para inspeção
        save_file(kmz_data, 'output.kmz')

        # Verificar se o conteúdo é um KMZ válido
        if validate_kmz_content(kmz_data):
            # Passo 2: Extrair o arquivo KML do KMZ
            with zipfile.ZipFile(io.BytesIO(kmz_data)) as kmz:
                # Encontrar o arquivo KML dentro do KMZ
                kml_filename = [name for name in kmz.namelist() if name.endswith('.kml')][0]
                with kmz.open(kml_filename) as kml_file:
                    kml_data = kml_file.read()

            # Passo 3: Ler o arquivo KML
            root = ET.fromstring(kml_data)

            # Exemplo: Imprimir todos os nomes dos elementos 'Placemark'
            for placemark in root.findall('.//{http://www.opengis.net/kml/2.2}Placemark'):
                name = placemark.find('{http://www.opengis.net/kml/2.2}name')
                if name is not None:
                    print(name.text)
        else:
            print("O conteúdo retornado não é um arquivo KMZ válido.")
    else:
        print(f"Falha ao acessar a URL: Status code {response.status_code}")



        
def callAPIs(api_url, session):
      
    try:    
        # Verifique se a requisição foi bem-sucedida (código de status 200)
        if session.get(api_url).status_code == 200:
            # Extraia o conteúdo JSON (ou texto) da resposta
            data = session.get(api_url).json()  # ou response.text se for texto
            
            return data                
        else:
            print(f"Falha ao acessar a API. Status Code: {session.post(api_url).status_code}")      
            
    finally:
        # Feche a sessão após o uso
        session.close()



session = autenticar()
print(session)

dataLinha = callAPIs("https://api.olhovivo.sptrans.com.br/v2.1/Linha/BuscarLinhaSentido?termosBusca=1705-10&sentido=1", session)
print(dataLinha)

data2 = callAPIs("https://api.olhovivo.sptrans.com.br/v2.1/Posicao", session)
print(data2)

#data3 = callAPIs("https://api.olhovivo.sptrans.com.br/v2.1/Posicao/Linha?codigoLinha=32934", session)
#print(data3)

data4 = callKmz("https://api.olhovivo.sptrans.com.br/v2.1/KMZ", session)
print(data4)
