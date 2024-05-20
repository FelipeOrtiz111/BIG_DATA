import apache_beam as beam
import requests
from bs4 import BeautifulSoup
from apache_beam.options.pipeline_options import PipelineOptions


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

def run_pipeline(input_url, output_path, project, region):
    options = PipelineOptions(
        flags=[],
        runner='DataflowRunner',
        project=project,
        region=region,
        temp_location='gs://bucket_gtfs/temp/',
        requirements_file='./requirements.txt',
        setup_file='./setup.py'
    )

    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "Create Input" >> beam.Create([input_url])
            | "Web Scraping" >> beam.ParDo(WebScrapeFn())
            | "Write to GCS" >> beam.io.WriteToText(output_path)
        )

if __name__ == '__main__':
    input_url = None  # No se utiliza, ya que el URL está definido dentro de la función de WebScrapeFn
    output_path = 'gs://bucket_gtfs/out/output.txt'
    project = 'redmetropolitana-423718'
    region = 'us-central1'
    
    run_pipeline(input_url, output_path, project, region)
