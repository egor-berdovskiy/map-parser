from .functions import format_address
from data.config import General

import os
import time
import pandas as pd


def main():
    directory_output = [file.replace('.xlsx', '') for file in os.listdir('./data/output/')]
    directory = [file for file in os.listdir('./data/raw/converted/') if file.replace('.xlsx', '') not in directory_output]

    for i_file, file in enumerate(directory):
        start = time.time()
        print(f'Start to formatting {file}.')
        print(f'Read file {file}.')
        df = pd.read_csv(f'./data/raw/converted/{file}')
        print(f'{file} - {df.shape[0]} length.')

        df['address'] = df['address'].apply(format_address)

        df = df.dropna()

        new_columns = {'kayttotarkoitus': 'building_type', 'kerrosluku': 'floor'}
        df = df.rename(columns=new_columns)

        df.to_excel(f'./data/output/{file}', index=False)
        end = time.time()
        print(f'===== [{i_file}/{len(directory)}] @ {file} converted: {end-start} sec. =====')

if __name__ == '__main__':
    main()
