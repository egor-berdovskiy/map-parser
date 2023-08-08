import asyncio
import os
import aiohttp
from random_user_agent.user_agent import UserAgent
import time
import pandas as pd
from datetime import datetime
import random
import json

from data.config import General

from operator import itemgetter


async def get_address(session, column, lat, lon, proxy, user_agent, log=False):
    url = f'{General.photon_url}/reverse?lon={lon}&lat={lat}'
    async with session.get(url=url) as response:
        if log: print(f'[{response.status}] {await response.text()}')
        location = await response.json()
        return {column: location}


async def main():
    print('Async converter!')
    directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/raw/converted/')]
    directory = [file for file in os.listdir('./data/raw/xlsx/') if file.replace('.xlsx', '') not in directory_output]

    print('Load proxy file')
    proxy_pool = []
    proxy_file = pd.read_csv('./data/hideme_proxy_export.csv', sep=';')
    for index, row in proxy_file.iterrows():
        proxy_pool.append(f'http://{row["host"]}:{row["port"]}')
    
    print('Starting!')
    user_agent = UserAgent()
    for i_file, file in enumerate(directory):
        start = time.time()
        date_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print(f'Start to converting {file}.')
        print(f'Read file {file}.')
        df = pd.read_excel(f'./data/raw/xlsx/{file}')
        print(f'{file} - {df.shape[0]} length.')
        
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            tasks = []
            for column, data in df['coordinates (lat, lon)'].items():
                lat, lon = data.split(' ')
                current_proxy = random.choice(proxy_pool)
                task = get_address(session, column, lat, lon, current_proxy, user_agent.get_random_user_agent(), log=True)
                tasks.append(task)
            results = await asyncio.gather(*tasks)

        # SORT
        addresses = sorted(results, key=lambda d: list(d.keys())[0])
        addresses_list = [list(d.values())[0] for d in addresses]

        df['address'] = addresses_list
        df.to_csv(f'./data/raw/converted/{file}', index=False)

        end = time.time()
        print(f'===== [{i_file}/{len(directory)}] @ {file} converted: {end-start} sec. =====')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
