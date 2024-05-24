import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions
import logging
import requests
import zipfile
import io
import os
from apache_beam.io.gcp.gcsio import GcsIO
import argparse

class FetchJsonFromUrl(beam.DoFn):
    def __init__(self, url):
        self.url = url

    def process(self, element):
        logging.info(f"Fetching JSON data from URL: {self.url}")
        response = requests.get(self.url)
        response.raise_for_status()
        data = response.json()
        resources = data['result']['resources']
        for resource in resources:
            if resource['url'].endswith('.zip'):
                logging.info(f"Found ZIP URL: {resource['url']}")
                yield resource['url']

class DownloadZip(beam.DoFn):
    def process(self, element):
        url = element
        logging.info(f"Downloading ZIP file from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logging.info(f"Downloaded ZIP file from URL: {url}")
        yield response.content, url  # Devolver también la URL

class SaveZipToGCS(beam.DoFn):
    def __init__(self, output_prefix):
        self.output_prefix = output_prefix

    def process(self, element):
        content, url = element
        file_name = os.path.basename(url)
        file_path = f'{self.output_prefix}/{file_name}'
        gcs = GcsIO()
        if not gcs.exists(file_path):  # Check if file already exists
            logging.info(f"Saving ZIP file to GCS: {file_path}")
            with gcs.open(file_path, 'wb') as f:
                f.write(content)
            logging.info(f"Saved ZIP file to GCS: {file_path}")
            yield file_path
        else:
            logging.info(f"File {file_path} already exists, skipping download.")
            yield file_path

class ExtractZip(beam.DoFn):
    def process(self, element):
        gcs = GcsIO()
        file_path = element
        logging.info(f"Extracting ZIP file: {file_path}")
        with gcs.open(file_path, 'rb') as f:
            content = f.read()
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                for zip_info in z.infolist():
                    if zip_info.filename.endswith('.txt'):
                        logging.info(f"Extracting file from ZIP: {zip_info.filename}")
                        with z.open(zip_info) as file:
                            yield (zip_info.filename, file.read(), os.path.basename(file_path))

class SaveExtractedFileToGCS(beam.DoFn):
    def __init__(self, output_prefix):
        self.output_prefix = output_prefix

    def process(self, element):
        file_name, content, zip_file_name = element
        extracted_folder = os.path.splitext(zip_file_name)[0]  # Obtener el nombre del archivo ZIP sin la extensión
        file_path = f'{self.output_prefix}/{extracted_folder}/{file_name}'  # Usar solo el nombre del archivo TXT
        gcs = GcsIO()
        logging.info(f"Saving extracted file to GCS: {file_path}")
        with gcs.open(file_path, 'wb') as f:
            f.write(content)
        logging.info(f"Saved extracted file to GCS: {file_path}")
        yield file_path

class DeleteZipFromGCS(beam.DoFn):
    def process(self, element):
        gcs = GcsIO()
        file_path = element
        if gcs.exists(file_path):
            logging.info(f"Deleting ZIP file from GCS: {file_path}")
            gcs.delete(file_path)
            logging.info(f"Deleted ZIP file from GCS: {file_path}")
        yield file_path

def run(argv=None):
    logging.info("Iniciando ejecución del pipeline...")
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_prefix', dest='output_prefix', required=True, help='Output directory prefix to save files.')
    parser.add_argument('--input_url', dest='input_url', required=True, help='Input URL containing JSON with URLs and file name prefixes.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Set Google Cloud options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'redmetropolitana-423718'
    google_cloud_options.staging_location = 'gs://gtfs_bucket1/staging/gtfs_historico'
    google_cloud_options.temp_location = 'gs://gtfs_bucket1/temp/gtfs_historico'

    

    with beam.Pipeline(options=pipeline_options) as p:
        # Leer el archivo de entrada con URLs y prefijos de nombres de archivos
        urls = (
            p
            | 'Start' >> beam.Create([None])  # Crear un PCollection inicial vacío
            | 'FetchJsonFromUrl' >> beam.ParDo(FetchJsonFromUrl(known_args.input_url))  # Extraer las URLs del JSON desde la URL
        )
        
        # Descargar archivos ZIP y guardarlos en GCS
        downloaded_files = (
            urls
            | 'DownloadZip' >> beam.ParDo(DownloadZip())
            | 'SaveZipToGCS' >> beam.ParDo(SaveZipToGCS(known_args.output_prefix))
        )
        
        # Extraer archivos TXT de los ZIP descargados
        extracted_files = (
            downloaded_files
            | 'ExtractZip' >> beam.ParDo(ExtractZip())
            | 'SaveExtractedFileToGCS' >> beam.ParDo(SaveExtractedFileToGCS(known_args.output_prefix))
        )
        
        # Eliminar los archivos ZIP después de la extracción
        _ = (
            downloaded_files
            | 'DeleteZipFromGCS' >> beam.ParDo(DeleteZipFromGCS())
        )
    logging.info("Ejecución del pipeline completada")
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
