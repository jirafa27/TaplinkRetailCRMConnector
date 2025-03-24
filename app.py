from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import logging
import sys
import hmac
import hashlib
from retailcrm_service import create_order_in_crm
import json

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
def process_taplink_webhook():
    """
    Обрабатывает вебхуки от Taplink
    """
    try:
        # Получаем тело запроса как строку
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
            result = create_order_in_crm(lead_data)
            return jsonify(result)
        else:
            return jsonify({
                'success': False,
                'error': f'Unsupported action: {action}'
            }), 400
            
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


