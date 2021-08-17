import json
import os
import shutil

import requests
from pqdm.processes import pqdm

CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))

def fetch_file(url, target):
    file_name = f'{CURRENT_PATH}/data/{target}'
    remote_file = requests.get(url)
    with open(file_name, 'wb') as f:
        f.write(remote_file.content)
    # TODO: Deal with potential writes to the same directory
    shutil.unpack_archive(file_name, f'{CURRENT_PATH}/data/', 'zip')
    os.remove(file_name)
    return None

def process_dataset(entry):
    fetch_file(entry['url'], entry['dataset'])
    print(f"processing: {entry['dataset']}")

def import_data():
    with open(f'{CURRENT_PATH}/catalog.json') as f:
        catalog_file = json.load(f)
    result = pqdm(catalog_file, process_dataset, n_jobs=2)

