from .functions import get_municipalities, order
from data.config import General


def main():
    # Берем муниципалитеты
    municipalities = get_municipalities()

    array = [{'id': i['id'], 'en': i['en']} for i in municipalities]  # Общий список из 309 муниципалитетов
    mun10 = [array[i:i+10] for i in range(0, len(array), 10)]  # Список по 10 муниципалитетов

    # Оформляем заказ
    order(email=General.email, municipalities=mun10)

if __name__ == '__main__':
    main()
