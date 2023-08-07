import asyncio
import os
import aiohttp
from random_user_agent.user_agent import UserAgent
import time
import pandas as pd
from datetime import datetime
import random

import requests


async def get_address(session, lat, lon, proxy, user_agent):
    url = f'https://photon.komoot.io/reverse?lon={lon}&lat={lat}'
    async with session.get(url=url) as response:
        print(response.status)
        return await response.text()
            


async def main():
    print('Async converter!')
    directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/output/')]
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
                task = get_address(session, lat, lon, current_proxy, user_agent.get_random_user_agent())
                tasks.append(task)
            results = await asyncio.gather(*tasks)

        result = []
        for column, response_text in zip(df['coordinates (lat, lon)'], results):
            result.append({column: response_text})

        print(result)

        end = time.time()
        print(f'===== [{i_file}/{len(directory)}] @ {file} converted: {end-start} sec. =====')
        break


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
