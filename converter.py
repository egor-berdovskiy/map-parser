import os
import time
import pandas as pd
import numpy as np
from typing import Union
import multiprocessing

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable

import random


class WorkManager:
    def __init__(self, proxy_list: list) -> None:
        self.directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/output/')]
        self.directory = [file for file in os.listdir('./data/raw/xlsx/') if file.replace('.xlsx', '') not in self.directory_output]
        self.workers = []

        self.proxy = proxy_list
        self.workers_count = len(self.proxy)
    
    def start(self):
        result_queue = multiprocessing.Queue()

        print(f'Wait for the workers to initialize')
        for x in range(self.workers_count):  # Инициализация воркеров
            self.workers.append(
                Worker(
                    id=x,
                    proxy=self.proxy[x],
                    result=result_queue
                )
            )
        print(f'{len(self.workers)} Workers initialize.')

        for i_file, file in enumerate(self.directory):  # Перебор файлов
            start = time.time()
            print(f'Start to converting {file}.')

            print(f'Read file {file}.')
            df = pd.read_excel(f'./data/raw/xlsx/{file}')
            print(f'{file} - {df.shape[0]} length.')

            print(f'Split the {file} into {self.workers_count} pieces.')
            chunks = np.array_split(df, self.workers_count)

            print(f'Give work workers.')
            for i, chunk in enumerate(chunks):  # Даём работу воркерам
                self.workers[i].work = chunk['coordinates (lat, lon)']

            processes = []

            for worker in self.workers:
                process = multiprocessing.Process(target=worker.do_work)
                processes.append(process)
                process.start()

            for process in processes:
                process.join()

            print(f'Result processing.')
            results = []
            while not result_queue.empty():
                results.append(result_queue.get())
            sorted_data = sorted(results, key=lambda x: list(x.keys())[0])

            merged_data = [address for item in sorted_data for addresses in item.values() for address in addresses]

            df['address'] = merged_data
            df.to_excel(f'./data/output/{file}', index=False)

            end = time.time()
            print(f'===== [{i_file}/{len(self.directory)}] @ {file} converted: {end-start} sec. =====')
            # break


class Worker(WorkManager):
    def __init__(self, id: int, proxy: dict, work: Union[list, None] = None, result: multiprocessing.Queue = None) -> None:
        self.id = id
        self.proxy = proxy
        self.work = work
        self.result = result

        self.proxy_pool = []
        proxy_file = pd.read_csv('./data/hideme_proxy_export.csv', sep=';')
        for index, row in proxy_file.iterrows():
            self.proxy_pool.append({'http': f'{row["ip"]}:{row["port"]}'})

    def do_work(self):
        # self.result = None
        # print(f'Worker {self.id}: Hey! Im going to get to work.')

        result = {self.id: []}

        for coordinate in self.work:  #! [::-1]:
            current_coordinate = coordinate.replace(' ', ', ')

            location = None
            
            try:  # Пробуем
                geolocator = Nominatim(user_agent='geoapiExercises', timeout=15, proxies=self.proxy)
                location = geolocator.reverse(current_coordinate, language='en')
            except (GeocoderServiceError, GeocoderTimedOut, TimeoutError, GeocoderUnavailable) as ex:  # При ошибке
                self.proxy = random.choice(self.proxy_pool)
                # ! print(f'[!] Worker {self.id}: Trying new proxy.')
                try:
                    geolocator = Nominatim(user_agent='geoapiExercises', timeout=15, proxies=self.proxy)
                    location = geolocator.reverse(current_coordinate, language='en', exactly_one=True)
                except:
                    result[self.id].append('error')
            except Exception as ex:
                print(f'[!] Worker {self.id}: Oh shit! Error: {ex}')
                result[self.id].append('error')

            if location:
                try:
                    address = location.raw.get('display_name')
                    #! print(address)
                    result[self.id].append(address)
                except:
                    result[self.id].append('error')
            else:
                result[self.id].append('error')

        if self.result is not None:
            self.result.put(result)


    def clear(self):
        self.work = None

