from configparser import ConfigParser


parser = ConfigParser()
parser.read(r'../config.ini')


class General:
    section = 'General'

    email = parser.get(section, 'email')
    purchases_url = parser.get(section, 'purchases_url')
    parse_layer = parser.get(section, 'parse_layer')
    photon_url = parser.get(section, 'photon_url')
