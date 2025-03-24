import os
from dotenv import load_dotenv
import requests
import logging
import sys
import json
import retailcrm
import time
from datetime import datetime

# Настройка логирования
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
RETAILCRM_API_KEY = os.getenv('RETAILCRM_API_KEY')
RETAILCRM_URL = os.getenv('RETAILCRM_URL')

# Инициализация клиента RetailCRM v5
crm = retailcrm.v5(RETAILCRM_URL, RETAILCRM_API_KEY)

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



def get_customer_by_phone(phone):
    """
    Получает данные клиента из RetailCRM по номеру телефона
    """
    try:
        response = crm.customers(filters={'phone': phone})
        response_data = response.get_response()
        if response_data.get('success'):
            customers = response_data.get('customers', [])
            return customers[0] if customers else None
        return None
    except Exception as e:
        logger.error(f"Error getting customer from RetailCRM: {str(e)}")
        return None

def create_customer_in_crm(customer_data):
    """
    Создает нового клиента в RetailCRM
    """
    try:
        # Формируем данные клиента
        customer = {
            'firstName': customer_data.get('firstName', ''),
            'lastName': customer_data.get('lastName', ''),
            'patronymic': customer_data.get('patronymic', ''),
            'email': customer_data.get('email'),
            'phones': [{
                'number': customer_data.get('phone')
            }],
            'address': {
                'text': customer_data.get('address'),
                'city': customer_data.get('city'),
                'street': customer_data.get('street'),
                'building': customer_data.get('building'),  # Дом
                'flat': customer_data.get('flat'),  # Номер квартиры/офиса
                'floor': customer_data.get('floor'),  # Этаж
                'block': customer_data.get('block'),  # Подъезд
                'house': customer_data.get('house'),  # Строение
                'housing': customer_data.get('housing'),  # Корпус
                'countryIso': 'RU'
            },
            'contragent': {
                'contragentType': 'individual'
            },
            'source': {
                'source': 'taplink',
                'medium': 'web'
            }
        }
        
        response = crm.customer_create(customer)
        response_data = response.get_response()
        if response_data.get('success'):
            return response_data
        else:
            logger.error(f"Error creating customer in RetailCRM: {response_data.get('errorMsg')}")
            return None
    except Exception as e:
        logger.error(f"Error creating customer in RetailCRM: {str(e)}")
        return None
    

def format_address(address_data: dict) -> str:
    """
    Форматирует адрес из компонентов в строку
    """
    address_parts = []
    address_mapping = {
        'city': None,
        'street': None,
        'building': 'д.',
        'housing': 'корп.',
        'house': 'стр.',
        'flat': 'кв./офис',
        'block': 'подъезд',
        'floor': 'этаж'
    }
    
    for field, prefix in address_mapping.items():
        if value := address_data.get(field):
            address_parts.append(f"{prefix + ' ' if prefix else ''}{value}")
    
    return ', '.join(filter(None, address_parts))

def get_address_changes(current_address: dict, new_address: dict) -> dict:
    """
    Определяет изменения в адресе клиента
    """
    changes = {}
    for key, value in new_address.items():
        if str(current_address.get(key)) != str(value):
            changes[key] = value
    
    if changes:
        changes['text'] = format_address(new_address)
    
    return changes

def get_customer_changes(current_data: dict, new_data: dict) -> dict:
    """
    Определяет изменения в данных клиента
    """
    changes = {}
    
    # Проверяем изменения в основных полях
    for field in ['firstName', 'lastName', 'patronymic']:
        if current_data.get(field) != new_data.get(field):
            changes[field] = new_data.get(field)
    
    # Проверяем изменения в адресе
    current_address = current_data.get('address', {})
    new_address = {
        'city': new_data.get('city'),
        'street': new_data.get('street'),
        'building': new_data.get('building'),
        'flat': new_data.get('flat'),
        'floor': new_data.get('floor'),
        'block': new_data.get('block'),
        'house': new_data.get('house'),
        'housing': new_data.get('housing')
    }
    
    address_changes = get_address_changes(current_address, new_address)
    changes['address'] = address_changes
    
    return changes

def create_or_update_customer_in_crm(customer_data: dict) -> dict:
    """
    Обновляет данные клиента в RetailCRM
    
    Args:
        customer_data (dict): Данные клиента для обновления
        
    Returns:
        dict: Обновленные данные клиента или None в случае ошибки
    """
    phone = customer_data.get('phone')
    if not phone:
        logger.error("No phone number provided")
        return None
        
    # Получаем текущие данные клиента и создаем нового, если клиента не существует
    customer_data_crm = get_customer_by_phone(phone)
    if not customer_data_crm:
        response = create_customer_in_crm(customer_data)
        if response and response.get('success'):
            # Получаем обновленные данные клиента
            customer_data_crm = get_customer_by_phone(phone)
            if not customer_data_crm:
                logger.error("Failed to get created customer data")
                return None
        else:
            logger.error("Failed to create customer")
            return None
    
    # Определяем изменения в данных клиента
    changes = get_customer_changes(customer_data_crm, customer_data)
    
    # Если есть изменения, обновляем данные
    if any(changes.values()):
        logger.info(f"Customer {customer_data_crm['id']} has changes: {changes}")
        
        # Обновляем основные данные
        for field in ['firstName', 'lastName', 'patronymic']:
            if field in changes:
                customer_data_crm[field] = changes[field]
        
        # Обновляем адрес
        if changes['address']:
            customer_data_crm['address'].update(changes['address'])
        
        try:
            # Отправляем обновление в RetailCRM
            response = crm.customer_edit(customer_data_crm, uid_type='id')
            if response.get_response().get('success'):
                logger.info(f"Successfully updated customer {customer_data_crm['id']} in RetailCRM")
                return customer_data_crm
            
            logger.error(f"Failed to update customer {customer_data_crm['id']} in RetailCRM")
            return None
            
        except Exception as e:
            logger.error(f"Error updating customer {customer_data_crm['id']} in RetailCRM: {str(e)}")
            return None
    
    return customer_data_crm




    

def prepare_order_data(customer_data_crm, items, total_sum):
    """
    Подготавливает данные для создания заказа
    
    Args:
        customer_data_crm (dict): Данные клиента из RetailCRM
        items (list): Список товаров
        total_sum (float): Общая сумма заказа
        
    Returns:
        dict: Данные для создания заказа
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    order_data = {
        'number': f"TAP-{int(time.time())}",
        'externalId': f"test-taplink-{int(time.time())}",
        'privilegeType': 'none',
        'countryIso': 'RU',
        'createdAt': current_time,
        'statusUpdatedAt': current_time,
        'lastName': customer_data_crm.get('lastName', ''),
        'firstName': customer_data_crm.get('firstName', ''),
        'patronymic': customer_data_crm.get('patronymic', ''),
        'phone': customer_data_crm.get('phone', ''),
        'email': customer_data_crm.get('email', ''),
        'call': False,
        'expired': False,
        'customerComment': customer_data_crm.get('comment', ''),
        'managerComment': f"Промокод: {customer_data_crm.get('promo_code', '')}" if customer_data_crm.get('promo_code') else None,
        'contragent': {
            'contragentType': 'individual'
        },
        'orderType': 'main',
        'orderMethod': 'taplink',
        'status': 'new',
        'customer': {
            'id': customer_data_crm['id'],
            'site': 'taplink2'
        },
        'contact': {
            'id': customer_data_crm['id'],
            'site': 'taplink2'
        },
        'delivery': {
            'code': 'courier',
            'cost': 0,
            'netCost': 0,
            'address': {
                'text': customer_data_crm.get('address', {}).get('text', ''),
                'city': customer_data_crm.get('address', {}).get('city', ''),
                'street': customer_data_crm.get('address', {}).get('street', ''),
                'building': customer_data_crm.get('address', {}).get('building', ''),
                'flat': customer_data_crm.get('address', {}).get('flat', ''),
                'floor': customer_data_crm.get('address', {}).get('floor', ''),
                'block': customer_data_crm.get('address', {}).get('block', ''),
                'house': customer_data_crm.get('address', {}).get('house', ''),
                'housing': customer_data_crm.get('address', {}).get('housing', ''),
                'countryIso': 'RU'
            },
            'date': customer_data_crm.get('delivery_date'),
            'time': {
                'from': customer_data_crm.get('delivery_time'),
                'to': customer_data_crm.get('delivery_time')
            }
        },
        'payments': [{
            'type': customer_data_crm.get('payment_type', 'cash'),
            'status': 'not-paid',
            'amount': total_sum
        }],
        'totalSumm': total_sum,
        'source': {
            'source': 'taplink',
            'medium': 'web'
        },
        'items': items,
        'fromApi': True,
        'shipped': False,
        'customFields': []
    }
    
    return order_data


def prepare_order_items(items):
    """
    Подготавливает товары для заказа
        
    Args:
        items (list): Список товаров из Taplink
        
    Returns:
        tuple: (список подготовленных товаров, общая сумма)
    """
    available_items = []
    total_sum = 0
    
    for item in items:
        article = item.get('article')
        nominal = item.get('nominal')
        requested_quantity = item.get('quantity', 1)
        price = item.get('price', 0)
        
        if not article or not nominal:
            logger.error(f"Missing article or nominal in item: {item}")
            continue
            
        # Получаем информацию о товаре из маппинга
        product_name = PRODUCTS_MAPPING.get(article)
        if not product_name:
            logger.error(f"Product not found in mapping: {article}")
            continue
        
        # Добавляем информацию о товаре
        available_items.append({
            'initialPrice': price,
            'quantity': requested_quantity,
            'productName': product_name,
            'offer': {
                'externalId': f"{article}-{nominal}",
                'xmlId': f"{article}-{nominal}"
            },
            'status': 'new',
            'ordering': len(available_items) + 1,
            'vatRate': 'none',
            'properties': [],
            'purchasePrice': price,
            'comment': '',
            'markingCodes': [],
            'externalIds': []
        })
        
        total_sum += requested_quantity * price
    
    return available_items, total_sum


def process_order_data(order_data: dict) -> dict:
    """
    Преобразует данные заказа из формата Taplink в формат для RetailCRM
    
    Args:
        order_data (dict): Данные заказа из Taplink
        
    Returns:
        dict: Преобразованные данные заказа
    """
    try:
        # Получаем данные из webhook
        records = order_data.get('records', [])
        
        customer_data = {}  
        # Извлекаем данные клиента из records
        for record in records:
            title = record.get('title', '')
            value = record.get('value', '')
            title = record.get('title', '')
            
            if title == 'Имя':  # Имя
                customer_data['firstName'] = value
            elif title == 'Фамилия':
                customer_data['lastName'] = value
            elif title == 'Отчество':
                customer_data['patronymic'] = value
            elif title == 'Телефон':  # Телефон
                customer_data['phone'] = value
            elif title == 'Время доставки':  # Время доставки
                customer_data['delivery_time'] = value
            elif title == 'Дата доставки':  # Дата доставки
                customer_data['delivery_date'] = value
            elif title == 'Способ оплаты':  # Способ оплаты
                customer_data['payment_type'] = value.lower()
            elif title == 'Примечание':
                customer_data['comment'] = value
            elif title == 'Промокод':
                customer_data['promo_code'] = value
            elif title == 'Город':
                customer_data['city'] = value
            elif title == 'Улица':
                customer_data['street'] = value
            elif title == 'Дом':
                customer_data['building'] = value
            elif title == 'Кв./офис':
                customer_data['flat'] = value
            elif title == 'Этаж':
                customer_data['floor'] = value
            elif title == 'Подъезд':
                customer_data['block'] = value
            elif title == 'Корпус':
                customer_data['housing'] = value
            elif title == 'Строение':
                customer_data['house'] = value
        
        # Формируем полный адрес
        address_parts = []
        if customer_data['city']:
            address_parts.append(customer_data['city'])
        if customer_data['street']:
            address_parts.append(f"ул. {customer_data['street']}")
        if customer_data['building']:
            address_parts.append(f"д. {customer_data['building']}")
        if customer_data['housing']:
            address_parts.append(f"корп. {customer_data['housing']}")
        if customer_data['house']:
            address_parts.append(f"стр. {customer_data['house']}")
        if customer_data['flat']:
            address_parts.append(f"кв. {customer_data['flat']}")
        if customer_data['block']:
            address_parts.append(f"подъезд {customer_data['block']}")
        if customer_data['floor']:
            address_parts.append(f"этаж {customer_data['floor']}")
            
        customer_data['address'] = ', '.join(address_parts)
        
        # Преобразуем товары
        items = []
        for offer in order_data.get('offers', []):
            # Ищем артикул товара по названию в маппинге
            article = None
            for art, name in PRODUCTS_MAPPING.items():
                if name == offer.get('title'):
                    article = art
                    break
            
            if article:
                items.append({
                    'article': article,
                    'nominal': '1',  # Используем значение по умолчанию
                    'quantity': int(offer.get('amount', 1)),
                    'price': float(offer.get('price', 0))
                })
            else:
                logger.warning(f"Product not found in mapping: {offer.get('title')}")
        
        return {
            'customer': customer_data,
            'items': items
        }
        
    except Exception as e:
        logger.error(f"Error processing order data: {str(e)}")
        raise


def create_order_in_crm(order_data):
    """
    Обрабатывает заказ и создает его в RetailCRM
    
    Args:
        order_data (dict): Данные заказа
        
    Returns:
        dict: Результат обработки заказа
    """
    try:
        # Преобразуем данные заказа
        order_data = process_order_data(order_data)
        # Обновляем или создаем клиента
        customer_data_crm = create_or_update_customer_in_crm(order_data['customer'])
        if not customer_data_crm:
            logger.error("Failed to create/update customer")
            return {
                'success': False,
                'error': 'Failed to create/update customer',
                'items': []
            }
        
        # Подготавливаем товары
        available_items, total_sum = prepare_order_items(order_data['items'])
        logger.info(f"Prepared items: {json.dumps(available_items, indent=2)}")
        
        if not available_items:
            logger.error("No valid items after preparation")
            return {
                'success': False,
                'error': 'No valid items after preparation',
                'items': []
            }
        
        # Подготавливаем данные заказа
        prepared_order_data = prepare_order_data(customer_data_crm, available_items, total_sum)
        
        # Логируем данные заказа для отладки
        logger.info(f"Prepared order data: {json.dumps(prepared_order_data, indent=2)}")
        
        # Создаем заказ в RetailCRM
        response = crm.order_create(prepared_order_data, site='taplink2')
        result = response.get_response()
        
        if result.get('success'):
            logger.info(f"Order created successfully in RetailCRM: {result}")
            return {
                'success': True,
                'order_id': result.get('id'),
                'items': available_items
            }
        else:
            error_msg = result.get('errorMsg', 'Unknown error')
            logger.error(f"Failed to create order in RetailCRM: {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'items': available_items
            }
            
    except Exception as e:
        logger.error(f"Error processing order: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'items': []
        }

def main():
    """
    Основная функция для создания тестового заказа
    """
    # Пример данных заказа
    order_data = {
            'profile_id': '5567465',
            'status_id': '1',
            'nickname': 'indeika_smr',
            'contact_id': '32192893',
            'block_id': '',
            'order_id': '25534116',
            'order_number': '1522',
            'order_version': '0',
            'order_status_id': '1',
            'purpose': 'МАНТЫ С КАРТОШКОЙ И КАПУСТОЙ',
            'tms_modify': '2025-03-24T12:49:57Z',
            'budget': '560',
            'currency_title': '₽',
            'currency_code': 'RUB',
            'utm_source': '',
            'utm_medium': '',
            'utm_campaign': '',
            'utm_content': '',
            'utm_term': '',
            'page_link': 'https://taplink.cc/indeika_smr/m/',
            'page_title': 'Товары',
            'lead_id': '52658682',
            'ip': '213.139.74.94',
            'lead_number': '1594',
            'date_created': '2025-03-24',
            'tms_created': '2025-03-24T12:49:57Z',
            'records': [
                {'type': '3', 'value': 'Тест', 'idx': '1', 'title': 'Имя'},
                {'type': '1', 'value': 'Тест', 'idx': '19', 'title': 'Фамилия'},
                {'type': '1', 'value': 'Тестович', 'idx': '21', 'title': 'Отчество'},
                {'type': '7', 'value': '79001234567', 'idx': '3', 'title': 'Телефон'},
                {'type': '5', 'value': '13:00', 'idx': '4', 'title': 'Время доставки'},
                {'type': '13', 'value': '28.03.2025', 'idx': '5', 'title': 'Дата доставки '},
                {'type': '8', 'value': 'QR-код', 'idx': '6', 'title': 'Способ оплаты'},
                {'type': '1', 'value': 'Пу', 'idx': '17', 'title': 'Примечание'},
                {'type': '1', 'value': 'пу', 'idx': '18', 'title': 'Промокод'},
                {'type': '1', 'value': 'Москва', 'idx': '7', 'title': 'Город'},
                {'type': '1', 'value': 'фУЦКПЕЫИ', 'idx': '8', 'title': 'Улица'},
                {'type': '1', 'value': '1', 'idx': '12', 'title': 'Дом'},
                {'type': '1', 'value': '1', 'idx': '13', 'title': 'Корпус'},
                {'type': '1', 'value': '1', 'idx': '16', 'title': 'Строение'},
                {'type': '1', 'value': '12', 'idx': '11', 'title': 'Кв./офис'},
                {'type': '1', 'value': '1', 'idx': '14', 'title': 'Подъезд'},
                {'type': '1', 'value': '1', 'idx': '15', 'title': 'Этаж'}
            ],
            'email': '',
            'phone': '79001234567',
            'fullname': 'Тест',
            'records_extended': [
                {'idx': 'a', 'name': 'lead_number', 'type': 'number', 'value': '1594'},
                {'idx': 'b', 'name': 'contacts', 'type': 'text', 'value': [
                    'Имя: Тест',
                    'Фамилия: Тест',
                    'Отчество: Тестович',
                    'Телефон: +79001234567',
                    'Время доставки: 13:00',
                    'Дата доставки : 28.03.2025',
                    'Способ оплаты: QR-код',
                    'Примечание: Пу',
                    'Промокод: пу',
                    'Город: Москва',
                    'Улица: фУЦКПЕЫИ',
                    'Дом: 1',
                    'Корпус: 1',
                    'Строение: 1',
                    'Кв./офис: 12',
                    'Подъезд: 1',
                    'Этаж: 1'
                ]},
                {'idx': 'c', 'name': 'cart', 'type': 'text', 'value': ['МАНТЫ С КАРТОШКОЙ И КАПУСТОЙ, 1 шт., 560.00 RUB']},
                {'idx': 'd', 'name': 'budget', 'type': 'number', 'value': '560'},
                {'idx': 'e', 'name': 'shipping', 'type': 'text'},
                {'idx': 'f', 'name': 'shipping_price', 'type': 'number', 'value': ''},
                {'idx': 'g', 'name': 'order_link', 'type': 'string', 'value': 'https://taplink.io/payments/1859ea4/'},
                {'idx': 'h', 'name': 'weight', 'type': 'number', 'value': '0'},
                {'idx': 'i', 'name': 'order_number', 'type': 'number', 'value': '1522'},
                {'idx': 'j', 'name': 'page_link', 'type': 'string', 'value': 'https://taplink.cc/indeika_smr/m/'},
                {'idx': 'k', 'name': 'discounts', 'type': 'string', 'value': ''}
            ],
            'offers': [
                {
                    'offer_id': '21188097',
                    'product_id': '12445287',
                    'title': 'МАНТЫ С КАРТОШКОЙ И КАПУСТОЙ',
                    'amount': '1',
                    'price': '560',
                    'budget': '560',
                    'price_discount': '',
                    'weight': '0'
                }
            ],
            'username': 'indeika_smr'
    }
    
    # Обрабатываем заказ
    result = create_order_in_crm(order_data)
    
    # Выводим результат
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main() 