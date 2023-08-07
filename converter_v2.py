import os
import time
import pandas as pd
import numpy as np
from typing import Union
import multiprocessing

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable, GeocoderRateLimited

import random

from functools import partial

import json

from datetime import datetime


def get_address(id: int, work: list, proxy_list: list):
	proxy = random.choice(proxy_list)

	result = {id: []}

	for coordinate in work:
		current_coordinate = coordinate.replace(' ', ', ')

		location = None

		try:
			geolocator = Nominatim(user_agent='lotus9200@gmail.com', timeout=1000, proxies=proxy)
			location = geolocator.reverse(current_coordinate, language='en', exactly_one=True)
		except GeocoderServiceError as ex:
			time.sleep(10)
			geolocator = change_proxy(random.choice(proxy_list))
			location = geolocator.reverse(current_coordinate, language='en', exactly_one=True)
		except GeocoderTimedOut as ex:
			time.sleep(random.randint(1 * 100, 10 * 100) /100)
			geolocator = change_proxy(random.choice(proxy_list))
			location = geolocator.reverse(current_coordinate, language='en', exactly_one=True)
		except GeocoderRateLimited as ex:
			time.sleep(15)
			geolocator = change_proxy(random.choice(proxy_list))
			location = geolocator.reverse(current_coordinate, language='en', exactly_one=True)
		except TimeoutError as ex:
			time.sleep(15)
			geolocator = change_proxy(random.choice(proxy_list))
			location = geolocator.reverse(current_coordinate, language='en', exactly_one=True)
		except Exception as ex:
			print(f'[!] Error: {ex}')
			result[id].append('error')

		# Format address
		result[id].append(format_address(location, True))  # print addr
		

	return result


def change_proxy(proxy):
	try:
		return Nominatim(user_agent='lotus9200@gmail.com', timeout=1000, proxies=proxy)
	except Exception as ex:
		print('[!] Error in change proxy: {ex}')


def format_address(location, show=False, log=False):
	if location:
		address = location.raw['address']
		full_address = location.raw.get('display_name')

		if show: print(full_address)  # Show cur address

		try:
			if address.get('house_number') and address.get('postcode'):
				return f"{address['road']} {address['house_number']}, {address['postcode']}, {address['city']}"
			elif address.get('postcode'):
				return f"{address['road']}, {address['postcode']}, {address['city']}"
			elif address.get('house_number'):
				return f"{address['road']} {address['house_number']}, {address['city']}"
			elif not (address.get('postcode') and address.get('house_number')):
				return f"{address['road']}, {address['city']}"
			else:
				return full_address
		except Exception as ex:
			if log: print(f'Error in location: {ex}')
			return full_address
	else:
		return 'NaN'


def end_func(response):
	return response


if __name__ == '__main__':
	num_processes = multiprocessing.cpu_count() * 6

	directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/output/')]
	directory = [file for file in os.listdir('./data/raw/xlsx/') if file.replace('.xlsx', '') not in directory_output]

	# Get proxy
	proxy_pool = []
	proxy_file = pd.read_csv('./data/hideme_proxy_export.csv', sep=';')
	for index, row in proxy_file.iterrows():
		proxy_pool.append({'http': f'{row["ip"]}:{row["port"]}'})

	# Read files
	for i_file, file in enumerate(directory):  # Перебор файлов
		start = time.time()
		date_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		print(f'Start to converting {file}.')
		print(f'Read file {file}.')
		df = pd.read_excel(f'./data/raw/xlsx/{file}')
		print(f'{file} - {df.shape[0]} length.')

		print(f'Split the {file} into {num_processes} pieces.')
		chunks = np.array_split(df['coordinates (lat, lon)'], num_processes)

		partial_get_address = partial(get_address, proxy_list=proxy_pool)
		with multiprocessing.Pool(num_processes) as pool:
			result = pool.starmap_async(partial_get_address, zip(range(num_processes), chunks), callback=end_func)
			pool.close()
			pool.join()

		data = None
		sorted_data = None
		merged_data = None
		
		try:
			data = result.get()
			if data:
				sorted_data = sorted(data, key=lambda x: list(x.keys())[0])
				merged_data = [address for item in sorted_data for addresses in item.values() for address in addresses]
			else:
				print('[!!!] Data is NaN')
		except Exception as ex:
			print(f'[!!!] Error when unpacking the result: {ex}')

		end = time.time()

		date_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		temp_data = {
			'date_start': date_start,
			'date_end': date_end,
			'convert_range': end-start,
			'sorted_data': sorted_data,
			'merged_data': merged_data,
			"lengths": (len(sorted_data), len(merged_data))
		}

		with open(f'./data/raw/temp/{file.replace(".xlsx", "")}.json', 'w', encoding='utf-8') as f:
			json.dump(temp_data, f, indent=4)

		try:
			df['address'] = merged_data
			df.to_excel(f'./data/output/{file}', index=False)
			print(f'===== [{i_file}/{len(directory)}] @ {file} converted: {end-start} sec. =====')
		except Exception as ex:
			print(f'Error: {ex}')
		