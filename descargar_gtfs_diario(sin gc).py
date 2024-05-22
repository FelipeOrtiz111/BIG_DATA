import requests # type: ignore
from bs4 import BeautifulSoup # type: ignore
import os
from urllib.parse import urljoin

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

url = "https://www.dtpm.cl/index.php/noticias/gtfs-vigente"
#res = requests.get(url)
#dir(res)

# solicitud http a la página
response = requests.get(url)

# creamos un objeto beautifulSoup 
soup = BeautifulSoup(response.content, "html.parser")

# buscamos el enlace del archivo, que contenga "GTFS" en el nombre
link_archivo = soup.find("a", href=True, string="GTFS")

if link_archivo:
    # obtenemos la url completa del archivo, y se la otorgamos a url_gtfs
    url_GTFS = "https://www.dtpm.cl" + link_archivo["href"]

    # descargamos el archivo
    response_gtfs = requests.get(url_GTFS)
    if response_gtfs.status_code == 200:
        # guardamos el archivo en el directorio actual
        with open("GTFS_diario.zip", "wb") as file:
            file.write(response_gtfs.content)
        print("Archivo descargado correctamente. ")
        
    else:
        print("Error al descargar el archivo. ")
else:
    print("No se encontró el enlace al archivo. ")
