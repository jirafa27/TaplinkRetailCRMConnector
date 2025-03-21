from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import requests
import logging
import sys
import hmac
import hashlib

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = Flask(__name__)

# Конфигурация
TAPLINK_WEBHOOK_SECRET = os.getenv('TAPLINK_WEBHOOK_SECRET')
RETAILCRM_API_KEY = os.getenv('RETAILCRM_API_KEY')
RETAILCRM_URL = os.getenv('RETAILCRM_URL')

# Словарь соответствия артикулов и наименований товаров
PRODUCTS_MAPPING = {
    '1': 'ПОДАРОЧНЫЙ СЕРТИФИКАТ',
    '2': 'ПЕЛЬМЕНИ ИЗ ИНДЕЙКИ КЛАССИЧЕСКИЕ',
    '3': 'ПЕЛЬМЕНИ ИЗ ИНДЕЙКИ РАВИОЛИ',
    '4': 'МАНТЫ РУБЛЕННЫЕ',
    '5': 'МАНТЫ ИЗ ИНДЕЙКИ С КАРТОШКОЙ',
    '6': 'КОТЛЕТЫ ИЗ ИНДЕЙКИ (В ПАНИРОВКЕ)',
    '7': 'КОТЛЕТЫ ИЗ ИНДЕЙКИ (БЕЗ ПАНИРОВКЕ)',
    '8': 'ЗРАЗЫ ИЗ ИНДЕЙКИ С СЫРОМ',
    '9': 'ЗРАЗЫ ИЗ ИНДЕЙКИ С ЯЙЦОМ',
    '10': 'ГОЛУБЦЫ ИЗ ИНДЕЙКИ',
    '11': 'ПЕРЦЫ ФАРШИРОВАННЫЕ ИЗ ИНДЕЙКИ',
    '12': 'ЛЮЛЯ КЕБЕБ ИЗ ИНДЕЙКИ',
    '13': 'ТЕФТЕЛИ ИЗ ИНДЕЙКИ С РИСОМ',
    '14': 'ФРИКАДЕЛЬКИ ИЗ ИНДЕЙКИ С РИСОМ',
    '15': 'ОТБИВНЫЕ В ПАНИРОВКЕ ИЗ ИНДЕЙКИ',
    '16': 'КУРНИК ИЗ ИНДЕЙКИ (1 шт.)',
    '17': 'ЭЧПОЧМАК ИЗ ИНДЕЙКИ (1 шт.)',
    '18': 'ПЕЛЬМЕНИ КУРИНЫЕ',
    '19': 'КОТЛЕТЫ КУРИНЫЕ',
    '20': 'КУПАТЫ КУРИНЫЕ',
    '21': 'СЫРНИКИ',
    '22': 'СЫРНИКИ С СЕМЕНАМИ ЧИА',
    '23': 'ЗАПЕКАНКА (1 шт., 200 гр.)',
    '24': 'ЗАПЕКАНКА С СЕМЕНАМИ ЧИА (1 шт., 200 гр.)',
    '25': 'БЛИНЧИКИ С КУРИЦЕЙ',
    '26': 'БЛИНЧИКИ С ТВОРОГОМ',
    '27': 'ВАРЕНИКИ С ТВОРОГОМ',
    '28': 'ВАРЕНИКИ С КАРТОШКОЙ',
    '29': 'ПЕРЦЫ ФАРШИРОВАННЫЕ РИСОМ И ГРИБАМИ',
    '30': 'ГОЛУБЦЫ С РИСОМ И ГРИБАМИ',
    '31': 'МАНТЫ С КАРТОШКОЙ И КАПУСТОЙ',
    '32': 'МАНТЫ С КАРТОШКОЙ И ГРИБАМИ',
    '33': 'МАНТЫ ИЗ ИНДЕЙКИ С ТЫКВОЙ',
    '34': 'СЫРНИКИ С ШОКОЛАДОМ'
}


def check_customer_exists(phone):
    """
    Проверка существования клиента в RetailCRM по номеру телефона
    """
    try:
        url = f"{RETAILCRM_URL}/customers"
        params = {
            'apiKey': RETAILCRM_API_KEY,
            'filter[phone]': phone
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get('success') and data.get('customers'):
            return data['customers'][0]  # Возвращаем первого найденного клиента
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error checking customer in RetailCRM: {str(e)}")
        return None


def create_order_in_crm(order_data):
    """
    Создание заказа в RetailCRM через API
    """
    try:
        # Формируем URL для создания заказа
        url = f"{RETAILCRM_URL}/orders/create"

        # Добавляем API ключ к запросу
        params = {
            'apiKey': RETAILCRM_API_KEY
        }

        # Отправляем POST запрос
        response = requests.post(url, json=order_data, params=params)
        response.raise_for_status()  # Проверяем на ошибки HTTP

        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating order in RetailCRM: {str(e)}")
        return None


def process_taplink_order(taplink_data):
    """
    Обработка заказа из Taplink и преобразование его в формат RetailCRM
    """
    try:
        # Получаем данные о заказе
        order_items = taplink_data.get('items', [])
        customer_data = taplink_data.get('customer', {})
        phone = customer_data.get('phone')

        # Проверяем существование клиента
        existing_customer = None
        if phone:
            existing_customer = check_customer_exists(phone)
            if existing_customer:
                logger.info(f"Found existing customer: {existing_customer.get('id')}")
            else:
                logger.info("Customer not found in RetailCRM")

        # Формируем позиции заказа для RetailCRM
        crm_items = []
        for item in order_items:
            base_article = item.get('article')
            nominal = item.get('nominal')
            quantity = item.get('quantity', 1)

            # Получаем наименование товара
            product_name = PRODUCTS_MAPPING.get(base_article, f"Товар с артикулом {base_article}")

            # Формируем артикул в формате RetailCRM
            crm_article = f"{base_article}-{nominal}"

            crm_items.append({
                'article': crm_article,
                'quantity': quantity,
                'price': item.get('price', 0),
                'name': product_name  # Добавляем наименование товара
            })

            logger.info(f"Processing order item: article={base_article}, nominal={nominal}, "
                        f"product_name={product_name}, crm_article={crm_article}, quantity={quantity}")

        # Формируем данные заказа для RetailCRM
        order_data = {
            'order': {
                'items': crm_items,
                'customer': {
                    'phone': phone,
                    'firstName': customer_data.get('name'),
                    'email': customer_data.get('email'),
                    'address': customer_data.get('address'),
                    'city': customer_data.get('city'),
                    'street': customer_data.get('street'),
                    'building': customer_data.get('building'),
                    'flat': customer_data.get('flat'),
                    'comment': customer_data.get('comment')
                }
            }
        }

        # Если клиент существует, добавляем его ID
        if existing_customer:
            order_data['order']['customer']['id'] = existing_customer['id']

        logger.info(f"Order processed successfully: {taplink_data}")

        return order_data
    except Exception as e:
        logger.error(f"Error processing order: {str(e)}")
        return None


@app.route('/')
def index():
    """
    Корневой маршрут для проверки работоспособности сервера
    """
    return jsonify({'status': 'ok', 'message': 'Taplink to RetailCRM connector is running'})


@app.route('/webhook/taplink', methods=['POST'])
def taplink_webhook():
    """
    Эндпоинт для получения вебхуков от Taplink
    """
    try:
        # Получаем тело запроса
        data = request.get_data()
        
        # Получаем подпись из заголовка
        signature = request.headers.get('taplink-signature')
        
        if not signature:
            logger.warning("No signature received in webhook request")
            return jsonify({'error': 'No signature provided'}), 401
            
        # Проверяем подпись
        expected_signature = hmac.new(
            TAPLINK_WEBHOOK_SECRET.encode('utf-8'),
            data,
            hashlib.sha1
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected_signature):
            logger.warning(f"Invalid webhook signature received: {signature}")
            return jsonify({'error': 'Invalid signature'}), 401

        # Парсим JSON данные
        webhook_data = request.get_json()
        logger.info(f"Received webhook from Taplink: {webhook_data}")

        # Проверяем тип события
        action = webhook_data.get('action')
        if action == 'leads.created':
            # Обработка нового лида
            lead_data = webhook_data.get('data', {})
            processed_order = process_taplink_order(lead_data)
            
            if processed_order:
                response = create_order_in_crm(processed_order)
                if response and response.get('success'):
                    logger.info(f"Order created successfully in RetailCRM: {response.get('id')}")
                    return jsonify({'success': True, 'crm_order_id': response.get('id')})
                else:
                    logger.error(f"Failed to create order in RetailCRM: {response}")
                    return jsonify({'error': 'Failed to create order in RetailCRM'}), 500
            else:
                logger.error(f"Failed to process lead data: {lead_data}")
                return jsonify({'error': 'Failed to process lead data'}), 400
        else:
            logger.info(f"Received unknown action: {action}")
            return jsonify({'message': 'Unknown action received'}), 200

    except Exception as e:
        logger.error(f"Unexpected error in webhook handler: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
