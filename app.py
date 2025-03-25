import os
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import hmac
import hashlib
import logging
import sys
from retailcrm_service import create_order_in_crm

# Настройка логирования
# Создаем директорию для логов, если она не существует
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Настраиваем корневой логгер
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Настраиваем форматтер
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Настраиваем вывод в файл
file_handler = logging.FileHandler(os.path.join(log_dir, 'app.log'), encoding='utf-8')
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

# Настраиваем вывод ошибок в отдельный файл
error_file_handler = logging.FileHandler(os.path.join(log_dir, 'error.log'), encoding='utf-8')
error_file_handler.setFormatter(formatter)
error_file_handler.setLevel(logging.ERROR)
root_logger.addHandler(error_file_handler)

# Настраиваем вывод в консоль
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

# Получаем логгер для текущего модуля
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
TAPLINK_WEBHOOK_SECRET = os.getenv('TAPLINK_WEBHOOK_SECRET')
RETAILCRM_API_KEY = os.getenv('RETAILCRM_API_KEY')
RETAILCRM_URL = os.getenv('RETAILCRM_URL')

app = Flask(__name__)

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


