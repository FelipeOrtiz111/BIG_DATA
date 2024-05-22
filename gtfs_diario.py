import os
import apache_beam as beam
import requests
from bs4 import BeautifulSoup
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import zipfile
import io


# Definición de la función DoFn para la extracción de datos
class WebScrapeFn(beam.DoFn):
    def process(self, element):
        # solicitud http a la página
        url = "https://www.dtpm.cl/index.php/noticias/gtfs-vigente"
        response = requests.get(url)
        # creamos un objeto beautifulSoup 
        soup = BeautifulSoup(response.content, "html.parser")
        # buscamos el enlace del archivo, que contenga "GTFS" en el nombre
        link_archivo = soup.find("a", href=True, string="GTFS")
        if link_archivo:
            # obtenemos la url completa del archivo, y se la otorgamos a url_gtfs
            url_GTFS = "https://www.dtpm.cl" + link_archivo["href"]
            # obtenemos el archivo
            response_gtfs = requests.get(url_GTFS)
            if response_gtfs.status_code == 200:
                yield response_gtfs.content
            else:
                yield "Error al descargar el archivo"
        else:
            yield "No se encontró el enlace al archivo"

# Clase para descomprimir el archivo ZIP y subirlo a GCS
class UnzipAndUploadFn(beam.DoFn):
    def process(self, element, output_path):
        if isinstance(element, bytes):
            # Utilizamos un buffer en memoria para el archivo zip
            with zipfile.ZipFile(io.BytesIO(element), 'r') as z:
                # itera sobre cada archivo dentro del zip
                for file_info in z.infolist():
                    with z.open(file_info) as f:
                        file_name = file_info.filename
                        file_content = f.read()
                        # Ruta completa del archivo en el bucket
                        file_path = f"{output_path}/{file_name}"
                        with FileSystems.create(file_path) as gcs_file:
                            gcs_file.write(file_content)
            yield f"Archivo {output_path} descomprimido y cargado a GCS"
        else:
            yield element

# Función principal para ejecutar el pipeline
def run_pipeline(input_url, output_path, project, region):
    options = PipelineOptions(
        flags=[],
        runner='DataflowRunner',
        project=project,
        region=region,
        temp_location='gs://gtfs_bucket1/temp/',        
        job_name='gtfs-datos-diarios',
        staging_location='gs://gtfs_bucket1/staging/',
        save_main_session=True,
     )

    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "Create Input" >> beam.Create([input_url])
            | "Web Scraping" >> beam.ParDo(WebScrapeFn())
            | "Unzip and Upload" >> beam.ParDo(UnzipAndUploadFn(), output_path=output_path)
        )

if __name__ == '__main__':
    input_url = None  # No se utiliza, ya que el URL está definido dentro de la función de WebScrapeFn
    output_path = 'gs://gtfs_bucket1/out/GTFS_Diarios'
    project = 'redmetropolitana-423718'
    region = 'us-central1'
    
    run_pipeline(input_url, output_path, project, region)
