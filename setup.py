import requests
from bs4 import BeautifulSoup

def test_requests():
    url = "https://www.dtpm.cl/index.php/noticias/gtfs-vigente"
    response = requests.get(url)
    if response.status_code == 200:
        print("Solicitud exitosa")
    else:
        print("Error en la solicitud")

test_requests()
