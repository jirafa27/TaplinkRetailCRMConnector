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
        print(data)
        logger.info(data)
        
        # Получаем подпись из заголовка
        signature = request.headers.get('taplink-signature')
        
        if not signature:
            logger.warning("No signature received in webhook request")
            return jsonify({'error': 'No signature provided'}), 401
            
        # Проверяем подпись (согласно документации)
        expected_signature = hmac.new(
            TAPLINK_WEBHOOK_SECRET.encode('utf-8'),
            data,
            hashlib.sha1
        ).hexdigest()
        
        if signature != expected_signature:
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
            
            # Формируем данные для RetailCRM
            order_data = {
                'customer': {
                    'name': lead_data.get('name', ''),
                    'phone': lead_data.get('phone', ''),
                    'email': lead_data.get('email', ''),
                    'address': lead_data.get('shipping', {}).get('addr1', ''),
                    'city': lead_data.get('shipping', {}).get('city', ''),
                    'comment': lead_data.get('records', [])
                },
                'items': lead_data.get('offers', []),
                'delivery': lead_data.get('shipping', {}),
                'discounts': lead_data.get('discounts', [])
            }
            
            # Получаем остатки из RetailCRM
            inventory = get_retailcrm_inventory()
            
            # Создаем заказ в RetailCRM
            result = process_taplink_order(order_data, inventory)
            
            if result['success']:
                logger.info(f"Order created successfully in RetailCRM: {result.get('order_id')}")
                return jsonify({
                    'success': True,
                    'order_id': result.get('order_id'),
                    'message': 'Order processed successfully'
                })
            else:
                logger.error(f"Failed to process lead data: {result.get('error')}")
                return jsonify({
                    'success': False,
                    'error': result.get('error')
                }), 400
        else:
            logger.info(f"Received unknown action: {action}")
            return jsonify({'message': 'Unknown action received'}), 200

    except Exception as e:
        logger.error(f"Unexpected error in webhook handler: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
