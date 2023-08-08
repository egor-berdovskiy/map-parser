import json
import requests
import geopandas as gpd
import pandas as pd
import fiona
from shapely.geometry import shape, MultiPolygon, Polygon
import json
import os
import pyproj
import utm

import asyncio
import time


def get_municipalities():
    response = requests.get('https://asiointi.maanmittauslaitos.fi/karttapaikka/api/spatialDataFiles/products')
    response = response.json()['kiinteistorekisterikartta_vektori']['municipalities']
    json_municipalities = json.dumps(response, indent=4)

    with open('./data/json/municipalities.json', 'w') as outfile:
        outfile.write(json_municipalities)

    return response


def order(email: str, municipalities):
    # Куки НЕ ДОЛГОВЕЧНЫ, если будете использовать повторно не забудьте обновить (в случае возникновения ошибки)
    # https://curlconverter.com
    # В консоли разработчика ищем запрос - createPayment > копировать > копировать все как cURL (bash)

    cookies = {
        'BIGipServerkakssasitu01-karttapaikka-pool': '2944673034.36895.0000',
        'i18next': 'en',
        '_pk_ref.10.0801': '%5B%22%22%2C%22%22%2C1690998723%2C%22https%3A%2F%2Fwww.google.com%2F%22%5D',
        '_pk_id.10.0801': '2317dc987081bc55.1690998723.',
        '_pk_ses.10.0801': '1',
        'XSRF-TOKEN': '9482ab5b-672b-4a2b-aca1-b690fd7c5443',
        'TS01ec89f6': '01b1a68b73e038f91a47b6dbf6caaaeb7515c366a026bfbd88b79ae115de5073129261c70e16cb3df4b5fc70ab238682f9e7c6c22fc8d95b4755f444d8640309e77bc3c16c45c5cef079f7a0c3246e1f0472abd105',
    }

    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        # 'Cookie': 'BIGipServerkakssasitu01-karttapaikka-pool=2944673034.36895.0000; i18next=en; _pk_ref.10.0801=%5B%22%22%2C%22%22%2C1690998723%2C%22https%3A%2F%2Fwww.google.com%2F%22%5D; _pk_id.10.0801=2317dc987081bc55.1690998723.; _pk_ses.10.0801=1; XSRF-TOKEN=9482ab5b-672b-4a2b-aca1-b690fd7c5443; TS01ec89f6=01b1a68b73e038f91a47b6dbf6caaaeb7515c366a026bfbd88b79ae115de5073129261c70e16cb3df4b5fc70ab238682f9e7c6c22fc8d95b4755f444d8640309e77bc3c16c45c5cef079f7a0c3246e1f0472abd105',
        'Origin': 'https://asiointi.maanmittauslaitos.fi',
        'Referer': 'https://asiointi.maanmittauslaitos.fi/karttapaikka/kassa/2',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 OPR/100.0.0.0',
        'X-XSRF-TOKEN': '9482ab5b-672b-4a2b-aca1-b690fd7c5443',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Opera GX";v="100"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'lang': 'en',
        'appContext': 'karttapaikka',
        'organizationId': None,
        'organizationName': None,
        'contactPerson': None,
        'firstName': '',
        'lastName': '',
        'phoneNumber': '',
        'email': f'{email}',
        'streetAddress': '',
        'postalCode': '',
        'city': '',
        'sessionId': '17bcc4c65d2630ded57f941924862676e7d5dae4869549e607443fac09c02578611430a90e1059fbae5eaa592a5e07c3a14ec251987b2f78bef7cf02b7ed5720',
        'purchaseRows': [],
        'spatialDataFilesOrders': [],
    }

    for list in municipalities:
        for mun in list:
            json_data['spatialDataFilesOrders'].append(
                {
                    'productType': 'fileDownload',
                    'description': f'{mun["en"]} - GeoPackage\nBuildings',
                    'price': '0',
                    'unmodifiable': True,
                    'name': 'Topographic database',
                    'productId': 'maastotietokanta',
                    'areaSelectionType': 'kunta',
                    'polygon': None,
                    'bbox': None,
                    'freeSelectionType': None,
                    'fileFormat': 'GPKG',
                    'theme': 'rakennukset',
                    'mapSheets': None,
                    'municipality': f'{mun["id"]}',
                    'year': 9999,
                },
            )

    response = requests.post(
        'https://asiointi.maanmittauslaitos.fi/karttapaikka/api/createPayment',
        cookies=cookies,
        headers=headers,
        json=json_data,
    )


def get_my_purchases(url):
    '''Получить список ваших покупок'''
    # Необходимо подождать пока в чеке загрузятся ваши карты, и потом уже вызывать эту функцию, это может занять время!
    # Пример ссылки на страницу загрузки: https://asiointi.maanmittauslaitos.fi/lataukset/api/spatialdatafilesorders/8aa5042f898eec620189b7911ab60923
    response = requests.get(url).json()['files']

    links = []

    for product in response:
        try:
            url = product['results']['results'][1]['path'] if product['results']['results'][0]['path'] is None else product['results']['results'][0]['path']
            current_product = {
                'name': product['inputs']['description'].split('\n')[0].replace(' - GeoPackage', ''),
                'url': url
                }
            links.append(current_product)
            print(current_product)
        except Exception as ex:
            print(f'Error: {ex}')

    json_purchases = json.dumps(links, indent=4)

    with open('data/json/purchases_link.json', 'w') as outfile:
        outfile.write(json_purchases)

    return links


def download_my_products(my_purchases):
    '''Скачивает ваши приобретенные карты, использует json объект получаемый из get_my_purchases'''
    print('Downloading files!')
    for (i, product) in enumerate(my_purchases):
        response = requests.get(url=product['url'])
        with open(f"data/raw/topographic_db/{product['name']}.gpkg", 'wb') as file:
            file.write(response.content)
        print(f'{i}/{len(my_purchases)} File {product["name"]}.gpkg saved.')


def get_layer(layer: str = 'rakennus', missing=False):
    '''Берет названия файлов из директории с картами, открывает их и сохраняет в /data/xlsx/ с определенными столбцами'''
    directory = []
    directory_xlsx = []

    if missing:
        directory_xlsx += [file.replace('.xlsx', '') for file in os.listdir('./data/raw/xlsx/')]
        directory = [file for file in os.listdir('./data/raw/topographic_db/') if file.replace('.gpkg', '') not in directory_xlsx]
    else:
        directory += os.listdir('./data/raw/topographic_db/')

    for i, file in enumerate(directory):
        df = pd.DataFrame()
        gdf = gpd.read_file(f'./data/raw/topographic_db/{file}', layer=layer)

        df = gdf[['kayttotarkoitus', 'kerrosluku', 'sijainti_piste']]
        df['ETRS-TM35FIN'] = pd.json_normalize(df['sijainti_piste'].apply(json.loads))['coordinates']
        df['ETRS-TM35FIN'] = df['ETRS-TM35FIN'].apply(lambda coordinates: str(coordinates).strip('[]').replace(',', ''))
        df['coordinates (lat, lon)'] = df['ETRS-TM35FIN'].apply(to_latlon)

        df = df.drop('sijainti_piste', axis=1)

        filename = file.replace('.gpkg', '')

        try:
            df.to_excel(f'./data/raw/xlsx/{filename}.xlsx', index=False)
            print(f'[{i}/{len(directory)}] | File {filename}.xlsx saved.')
        except Exception as ex:
            print(f'Error: {ex}')


def to_geojson(layer: str = 'rakennus', missing=False):
    directory = []
    directory_geojson = []

    if missing:
        directory_geojson += [file.replace('.geojson', '') for file in os.listdir('./data/geojson/')]
        directory = [file for file in os.listdir('./data/raw/topographic_db/') if file.replace('.gpkg', '') not in directory_geojson]
    else:
        directory += os.listdir('./data/raw/topographic_db/')

    for i, file in enumerate(directory):
        filename = file.replace('.gpkg', '')
        gdf = gpd.read_file(f'./data/raw/topographic_db/{file}', layer=layer)
        gdf.to_file(f'./data/geojson/{filename}.geojson', driver='GeoJSON')
        print(f'[{i}/{len(directory)}] | File {filename}.geojson saved.')


async def to_geojson_async(layer: str = 'rakennus', missing=False):
    start_time = time.time()
    directory = []
    directory_geojson = []

    if missing:
        directory_geojson += [file.replace('.geojson', '') for file in os.listdir('./data/geojson/')]
        directory = [file for file in os.listdir('./data/raw/topographic_db/') if file.replace('.gpkg', '') not in directory_geojson]
    else:
        directory += os.listdir('./data/raw/topographic_db/')

    async def process_file(file):
        filename = file.replace('.gpkg', '')
        gdf = gpd.read_file(f'./data/raw/topographic_db/{file}', layer=layer)
        gdf.to_file(f'./data/geojson/{filename}.geojson', driver='GeoJSON')
        print(f'File {filename}.geojson saved.')

    tasks = [process_file(file) for file in directory]
    await asyncio.gather(*tasks)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Total time taken: {elapsed_time:.2f} seconds')


def to_latlon(coordinates, log=False):
    coordinates = list(map(float, coordinates.split(' ')))  # Разделение координат по пробелу, пример: 19.023912_82.012391293
    if log: print(f'Converting "{coordinates}"...')  # Вывод конвертируемых кординат
    easting, northing = coordinates
    zone_number = 35
    zone_letter = 'V'
    lat = None
    lon = None

    try:  # Пробуем конвертировать обычным методом
        lat, lon = utm.to_latlon(easting, northing, zone_number, zone_letter)
    except utm.OutOfRangeError as ex:  # Если прошлый вариант не сработал
        etrs_tm35fin = pyproj.CRS.from_string("+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs")
        wgs84 = pyproj.CRS.from_epsg(4326)
        transformer = pyproj.Transformer.from_crs(etrs_tm35fin, wgs84, always_xy=True)

        lon, lat = transformer.transform(easting, northing)
    except Exception as ex:  # Если ничего не сработало
        print(f'Coordinate converting error: {ex}')
        return 'error'

    return f'{lat} {lon}'

def format_address(raw_address):
    try:
        address = eval(raw_address)
    except Exception as ex:
        print(f'error: {ex}')

    # Get type
    building_type = None
    try:
        building_type = address['features'][0]['properties']['type']
    except:
        return None
    
    # Get values
    street = f"{address['features'][0]['properties']['street']} " if 'street' in address['features'][0]['properties'].keys() else ''
    name = f"{address['features'][0]['properties']['name']} " if 'name' in address['features'][0]['properties'].keys() else ''
    housenumber = f"{address['features'][0]['properties']['housenumber']}, " if 'housenumber' in address['features'][0]['properties'].keys() else ''
    postcode = f"{address['features'][0]['properties']['postcode']}, " if 'postcode' in address['features'][0]['properties'].keys() else ''
    city = f"{address['features'][0]['properties']['city']}" if 'city' in address['features'][0]['properties'].keys() else ''

    if building_type == 'house':
        return f'{street}{name}{housenumber}{postcode}{city}'
    if building_type == 'locality':
        osm_value = f"{address['features'][0]['properties']['osm_value']}, " if 'osm_value' in address['features'][0]['properties'].keys() else ''
        country = f"{address['features'][0]['properties']['country']}" if 'country' in address['features'][0]['properties'].keys() else ''
        return f'{name}{osm_value}{country}'
    if building_type == 'street':
        return f"{street}{name}{postcode}{city}"
    if building_type == 'district':
        district = f"{address['features'][0]['properties']['district']}" if 'district' in address['features'][0]['properties'].keys() else ''
    else:
        return None

def get_type(raw_address):
    try:
        address = eval(raw_address)
    except Exception as ex:
        print(f'error: {ex}')

    try:
        building_type = address['features'][0]['properties']['type']
        return building_type
    except:
        return None
