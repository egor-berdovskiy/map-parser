import json
import requests


def get_municipalities():
    response = requests.get('https://asiointi.maanmittauslaitos.fi/karttapaikka/api/spatialDataFiles/products')
    response = response.json()['kiinteistorekisterikartta_vektori']['municipalities']
    json_municipalities = json.dumps(response, indent=4)

    with open('./data/json/municipalities.json', 'w') as outfile:
        outfile.write(json_municipalities)

    return response