import requests
import time
import logging

max_retries = 3
wait_time = 5

def load_data(url, out):
    for intento in range(max_retries):
        try:
            r = requests.get(url, stream=True, timeout=120)

            r.raise_for_status() 

            with open(out, 'wb') as archivo:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        archivo.write(chunk)
            return

        except (requests.exceptions.RequestException, IOError) as e:
            logging.warning(f"Error en el intento {intento + 1}.")
            
            if intento < max_retries - 1:
                time.sleep(wait_time) 
            else:
                logging.error(f"Máximo de intentos alcanzado ({url}).")
                raise