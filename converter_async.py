import asyncio
import os
import pandas as pd
import time
from datetime import datetime
import random

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable, GeocoderRateLimited



async def get_address(column, data, proxy_pool):
    current_proxy = random.choice(proxy_pool)
    coordinate = data.replace(' ', ', ')
    location = None

    try:
        geolocator = Nominatim(user_agent='lotus9200@gmail.com', timeout=1000, proxies=current_proxy)
        location = geolocator.reverse(coordinate, language='en', exactly_one=True)
    except (GeocoderServiceError, GeocoderTimedOut) as ex:
        time.sleep(15)
        geolocator = Nominatim(user_agent='lotus9200@gmail.com', timeout=1000, proxies=random.choice(proxy_pool))
        location = geolocator.reverse(coordinate, language='en', exactly_one=True)
    except Exception as ex:
        print('[!] Error: {ex}')
        return {column: 'error'}
    
    formatted_address = await format_address(location, True)
    return {column: formatted_address}
    

async def format_address(location, show=False, log=False):
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


async def main():
    print('Async converter!')
    directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/output/')]
    directory = [file for file in os.listdir('./data/raw/xlsx/') if file.replace('.xlsx', '') not in directory_output]

    print('Load proxy file')
    proxy_pool = []
    proxy_file = pd.read_csv('./data/hideme_proxy_export.csv', sep=';')
    for index, row in proxy_file.iterrows():
        proxy_pool.append({'http': f'{row["ip"]}:{row["port"]}'})
    
    print('Starting!')
    for i_file, file in enumerate(directory):
        start = time.time()
        date_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print(f'Start to converting {file}.')
        print(f'Read file {file}.')
        df = pd.read_excel(f'./data/raw/xlsx/{file}')
        print(f'{file} - {df.shape[0]} length.')
        
        for column, data in df['coordinates (lat, lon)'].items():
            values = await get_address(column, data, proxy_pool)

        end = time.time()
        print(values)
        print(f'===== [{i_file}/{len(directory)}] @ {file} converted: {end-start} sec. =====')
        break




if __name__ == '__main__':
    asyncio.run(main())
