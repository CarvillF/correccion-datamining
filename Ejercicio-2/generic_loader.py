import requests
from pathlib import Path


def load_data(url, out):
    r = requests.get(url, stream = True, timeout = 120)

    r.raise_for_status()

    with open(out, 'wb') as archivo:
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                archivo.write(chunk)