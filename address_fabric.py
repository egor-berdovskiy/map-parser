import multiprocessing
from functools import partial
import utm
import pandas as pd
import gspread
from geopy.geocoders import Nominatim
import os
import numpy as np
import time

import dask.dataframe as dd
import dask

def get_address(coordinates, proxy):
    lat, lon = coordinates.split(' ')

    location = None


    try:
        geolocator = Nominatim(user_agent="geoapiExercises", timeout=10, proxies=proxy)
        location = geolocator.reverse((lat, lon), language='en')
    except Exception as ex:
        print(f'Error: {ex}')
        return 'error'

    if location:
        address = location.raw['address']
        full_address = location.raw['display_name']
        try:
            if address.get('house_number') and address.get('postcode'):
                # print('[i] converted address...')
                return f"{address['road']} {address['house_number']}, {address['postcode']}, {address['city']}"
            elif address.get('postcode'):
                # print('[i] converted address...')
                return f"{address['road']}, {address['postcode']}, {address['city']}"
            elif address.get('house_number'):
                # print('[i] converted address...')
                return f"{address['road']} {address['house_number']}, {address['city']}"
            elif not (address.get('postcode') and address.get('house_number')):
                # print('[i] converted address...')
                return f"{address['road']}, {address['city']}"
            else:
                # print(f'[i] converted address...')
                return full_address
        except Exception as ex:
            # print(f'ERROR: {ex}\naddress: {address}\n')
            return full_address
    else:
        return 'error'
    

def get_address_exp(coordinates, proxy):
    lat, lon = coordinates.split(' ')

    location = None


    try:
        geolocator = Nominatim(user_agent="geoapiExercises", timeout=10, proxies=proxy)
        location = geolocator.reverse((lat, lon), language='en')
    except Exception as ex:
        print(f'Error: {ex}')
        return 'error'

    if location:
        address = location.raw['address']
        full_address = location.raw['display_name']
        try:
            if address.get('house_number') and address.get('postcode'):
                # print('[i] converted address...')
                return f"{address['road']} {address['house_number']}, {address['postcode']}, {address['city']}"
            elif address.get('postcode'):
                # print('[i] converted address...')
                return f"{address['road']}, {address['postcode']}, {address['city']}"
            elif address.get('house_number'):
                # print('[i] converted address...')
                return f"{address['road']} {address['house_number']}, {address['city']}"
            elif not (address.get('postcode') and address.get('house_number')):
                # print('[i] converted address...')
                return f"{address['road']}, {address['city']}"
            else:
                # print(f'[i] converted address...')
                return full_address
        except Exception as ex:
            # print(f'ERROR: {ex}\naddress: {address}\n')
            return full_address
    else:
        return 'error'


def worker(worker_id, work, proxy):
    print(f'Worker {worker_id} - Hi!\nMy work: {work[worker_id]}\n\n\n')
    current_works = work[worker_id]
    current_proxy = proxy[worker_id]
    for i, file in enumerate(current_works):
        filename = file.replace('.xlsx', '')
        df = pd.read_excel(f'./data/raw/xlsx/{file}')
        df['address'] = df['coordinates (lat, lon)'].apply(get_address, args=(current_proxy,))

        column_names = {
        'kayttotarkoitus': 'building_type',
        'kerrosluku': 'floor'
            }
        df.rename(columns=column_names, inplace=True)

        df.to_excel(f'./data/output/{filename}.xlsx')
        print(f'[{i}/{len(current_works)}] @ Worker {worker_id}: File done.')


def worker_dask(worker_id, work, proxy):
    print(f'Worker dask {worker_id} - Hi!\nMy work: {work[worker_id]}\n\n\n')
    current_works = work[worker_id]
    current_proxy = proxy[worker_id]
    for i, file in enumerate(current_works):
        filename = file.replace('.xlsx', '')

        parts = dask.delayed(pd.read_excel)(f'./data/raw/xlsx/{file}', sheet_name=0, usecols = [0, 1, 2, 3])
        df = dd.from_delayed(parts)
        df['address'] = df['coordinates (lat, lon)'].apply(get_address_exp, proxy=current_proxy, meta=('address', 'object'))

        column_names = {
        'kayttotarkoitus': 'building_type',
        'kerrosluku': 'floor'
            }
        df = df.rename(columns=column_names)

        df.compute().to_csv(f'./data/output/{filename}', index=False)
        print(f'[{i}/{len(current_works)}] @ Worker {worker_id}: File done.')
    

if __name__ == '__main__':
    proxy = [
        {'http': '213.209.133.65:63280', 'login': 'ucgXeK84', 'password': '4FGTCYH8'},
        {},
    ]

    directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/output/')]
    directory = [file for file in os.listdir('./data/raw/xlsx/') if file.replace('.xlsx', '') not in directory_output]

    workers_count = len(proxy)
    work = np.array_split(directory, workers_count)

    start_time = time.time()
    with multiprocessing.Pool(processes=workers_count) as pool:
        worker_func = partial(worker_dask, work=work, proxy=proxy)
        pool.map(worker_func, range(workers_count))
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Задача выполнилась за {execution_time:.6f} секунд.")