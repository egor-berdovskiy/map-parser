from .functions import get_my_purchases, download_my_products
from data.config import General


def main():
    links = get_my_purchases(url=General.purchases_url)  # Формируем список ссылок на продукты
    download_my_products(links)  # Проходимся по ссылкам и скачиваем продукты

if __name__ == '__main__':
    main()
