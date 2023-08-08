from .functions import get_layer, to_geojson
from data.config import General


def main():
    get_layer(missing=True, layer=General.parse_layer)
    to_geojson(missing=True, layer=General.parse_layer)

if __name__ == '__main__':
    main()
