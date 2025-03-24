from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import logging
import sys
import hmac
import hashlib
from create_order import create_order_in_crm

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
        logger.info(f"Received webhook data: {data.decode('utf-8')}")
        
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
            # Создаем заказ в RetailCRM
            create_order_in_crm(lead_data)
    except Exception as e:
        logger.error(f"Unexpected error in webhook handler: {str(e)}")
        return jsonify({'error': str(e)}), 500



            



if __name__ == '__main__':
    app.run(debug=True, port=5000)
