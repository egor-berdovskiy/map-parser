from converter import WorkManager
import pandas as pd

def main():
    proxy_df = pd.read_csv('./data/hideme_proxy_export.csv', sep=';')
    proxy = []

    for index, row in proxy_df.sample(100).iterrows():
        proxy.append({
            'http': f'{row["ip"]}:{row["port"]}'
        })
    
    wm = WorkManager(proxy)
    wm.start()


if __name__ == '__main__':
    main()