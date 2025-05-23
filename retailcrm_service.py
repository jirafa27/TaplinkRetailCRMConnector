import os
from dotenv import load_dotenv
import logging
import json
import retailcrm
import time
from datetime import datetime
import requests

# Настройка логирования
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
RETAILCRM_API_KEY = os.getenv('RETAILCRM_API_KEY')
RETAILCRM_URL = os.getenv('RETAILCRM_URL')

# Инициализация клиента RetailCRM v5
crm = retailcrm.v5(RETAILCRM_URL, RETAILCRM_API_KEY)




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
            'email': customer_data.get('email', ''),
            'phones': [{
                'number': customer_data.get('phone')
            }],
            'address': {
                'text': customer_data.get('address', ''),
                'city': customer_data.get('city', ''),
                'street': customer_data.get('street', ''),
                'building': customer_data.get('building', ''),  # Дом
                'flat': customer_data.get('flat', ''),  # Номер квартиры/офиса
                'floor': customer_data.get('floor', 0),  # Этаж
                'block': customer_data.get('block', 0),  # Подъезд
                'house': customer_data.get('house', ''),  # Строение
                'housing': customer_data.get('housing', ''),  # Корпус
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
            logger.info(f"Customer created in RetailCRM: {response_data}")
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
        # Пропускаем пустые значения в новом адресе
        if value is None or value == '' or value == 0:
            continue
            
        current_value = current_address.get(key)
        # Если текущее значение пустое, а новое нет - считаем изменением
        if current_value is None or current_value == '' or current_value == 0:
            changes[key] = value
            continue
            
        # Для непустых значений сравниваем как строки
        if str(current_value) != str(value):
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
    for field in ['firstName', 'lastName']:
        if current_data.get(field, '') != new_data.get(field, ''):
            changes[field] = new_data.get(field, '')
    
    # Проверяем изменения в адресе
    current_address = current_data.get('address', {})
    new_address = {
        'city': new_data.get('city', ''),
        'street': new_data.get('street', ''),
        'building': new_data.get('building', ''),
        'flat': new_data.get('flat', ''),
        'floor': new_data.get('floor', 0),
        'block': new_data.get('block', 0),
        'house': new_data.get('house', ''),
        'housing': new_data.get('housing', '')
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
            return customer_data_crm
        else:
            logger.error("Failed to create customer")
            return None
    
    # Определяем изменения в данных клиента
    changes = get_customer_changes(customer_data_crm, customer_data)
    
    # Если есть изменения, обновляем данные
    if any(changes.values()):
        logger.info(f"Customer {customer_data_crm['id']} has changes: {changes}")
        
        # Обновляем основные данные
        for field in ['firstName', 'lastName']:
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
            
            logger.error(f"Failed to update customer {customer_data_crm['id']} in RetailCRM {response.get_response()}")
            return None
            
        except Exception as e:
            logger.error(f"Error updating customer {customer_data_crm['id']} in RetailCRM: {str(e)}")
            return None
    
    return customer_data_crm



def get_offer(session, item):
    """
    Получает данные о торговом предложении из RetailCRM по его имени или по externalId и номиналу
    """
    
    try:
        if item.get('nominal'):
            external_id = f"1-{item.get('nominal')}"
            response = session.get(f"{RETAILCRM_URL}/api/v5/store/offers?filter[externalIds][]={external_id}")
        else:
            response = session.get(f"{RETAILCRM_URL}/api/v5/store/offers?filter[name]={item.get('title')}")
            
        response_data = response.json()
        if not response_data.get('success'):
            raise ValueError(f"Ошибка API RetailCRM: {response_data.get('errorMsg', 'Неизвестная ошибка')}")
            
        offers = response_data.get('offers', [])
        if not offers:
            raise IndexError(
                f"Торговое предложение не найдено: "
                f"{'externalId=1-' + item.get('nominal') if item.get('nominal') else 'name=' + item.get('title')}"
            )
            
        return offers[0]
        
    except requests.RequestException as e:
        logger.error(f"Ошибка при запросе к RetailCRM: {str(e)}")
        raise

    
    

def prepare_order_data(customer_data_crm, items, total_sum, manager_comment, extra_data, delivery_date):
    """
    Подготавливает данные для создания заказа
    Собирает все данные в финальную структуру заказа
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    order_data = {
        'number': f"TAP-{int(time.time())}",
        'externalId': f"taplink-{int(time.time())}",
        'privilegeType': 'none',
        'countryIso': 'RU',
        'createdAt': current_time,
        'statusUpdatedAt': current_time,
        'lastName': customer_data_crm.get('lastName', ''),
        'firstName': customer_data_crm.get('firstName', ''),
        'phone': customer_data_crm.get('phones')[0].get('number') if customer_data_crm.get('phones') else '',
        'email': customer_data_crm.get('email', ''),
        'call': False,
        'expired': False,
        'customerComment': customer_data_crm.get('comment', ''),
        'managerComment': manager_comment,
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
                'notes': extra_data,
                'text': customer_data_crm.get('address', {}).get('text', ''),
                'city': customer_data_crm.get('address', {}).get('city', ''), # Город
                'street': customer_data_crm.get('address', {}).get('street', ''), # Улица
                'building': customer_data_crm.get('address', {}).get('building', ''), # Дом
                'flat': customer_data_crm.get('address', {}).get('flat', ''), # Квартира
                'floor': customer_data_crm.get('address', {}).get('floor', 0), # Этаж
                'block': customer_data_crm.get('address', {}).get('block', 0), # Подъезд
                'house': customer_data_crm.get('address', {}).get('house', ''), # Корпус
                'housing': customer_data_crm.get('address', {}).get('housing', ''), # Строение
                'countryIso': 'RU'
            },
            'date': datetime.strptime(delivery_date, '%d.%m.%Y').strftime('%Y-%m-%d') if delivery_date else None,
            'time': {
                'from': customer_data_crm.get('delivery_time'),
                'to': customer_data_crm.get('delivery_time')
            }
        },
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
    """
    try:
        available_items = []
        total_sum = 0
        session = requests.Session()
        session.headers['X-API-KEY'] = RETAILCRM_API_KEY
        session.headers['Content-Type'] = 'application/x-www-form-urlencoded'
    
        manager_comment = ""
        for item in items:
            try:
                offer = get_offer(session, item)
            except IndexError as e:
                manager_comment += f"{str(e)}\n"
                continue
            offer_id = offer.get('id')
            nominal = item.get('nominal')
            requested_quantity = item.get('quantity', 1)
            price = offer.get('prices')[0].get('price')
    
            # Добавляем информацию о товаре
            available_items.append({
            'quantity': requested_quantity,
            'offer': {
                    'id': offer_id,
                }
            })
            if nominal:
                available_items[-1]['offer'].pop('id')
                available_items[-1]['offer']['externalId'] = f"1-{nominal}"

        
            total_sum += requested_quantity * price
        
        
    except Exception as e:
        logger.error(f"Error preparing order items: {str(e)}")
        return [], 0
    
    return available_items, total_sum, manager_comment


def process_order_data(order_data: dict) -> dict:
    """
    Преобразует данные заказа из формата Taplink в формат для RetailCRM
    """
    try:
        # Получаем данные из webhook
        records = order_data.get('records', [])
        
        customer_data = {}
        
        # Извлекаем данные клиента из records
        for record in records:
            title = record.get('title', '')
            value = record.get('value', '')
            
            if title == 'Имя':  # Имя
                customer_data['firstName'] = value
            elif title == 'Фамилия':
                customer_data['lastName'] = value
            elif title == 'Телефон':  # Телефон
                customer_data['phone'] = value
            elif title == 'Время доставки / примечание / промокод':
                customer_data['extra_data'] = value
            elif title == 'Дата доставки':  # Дата доставки
                customer_data['delivery_date'] = value
            elif title == 'Способ оплаты':  # Способ оплаты
                customer_data['payment_type'] = value
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
        if customer_data.get('city'):
            address_parts.append(customer_data['city'])
        if customer_data.get('street', None):
            address_parts.append(f"ул. {customer_data['street']}")
        if customer_data.get('building', None):
            address_parts.append(f"д. {customer_data['building']}")
        if customer_data.get('housing', None):
            address_parts.append(f"корп. {customer_data['housing']}")
        if customer_data.get('house', None):
            address_parts.append(f"стр. {customer_data['house']}")
        if customer_data.get('flat', None):
            address_parts.append(f"кв. {customer_data['flat']}")
        if customer_data.get('block', None):
            address_parts.append(f"подъезд {customer_data['block']}")
        if customer_data.get('floor', None):
            address_parts.append(f"этаж {customer_data['floor']}")
            
        customer_data['address'] = ', '.join(address_parts)
        
        # Преобразуем товары
        items = []
        for offer in order_data.get('offers', []):
            if offer.get('options'):
                for option in offer.get('options', []):
                    items.append({
                        'title': offer.get('title'),
                        'nominal': option.split(' ')[1],
                        'quantity': int(offer.get('amount', 1)),
                    })
            else:
                items.append({
                    'title': offer.get('title'),
                    'quantity': int(offer.get('amount', 1)),
                })

        
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
        if not order_data:
            return {
                'success': False,
                'error': 'Failed to process order data',
                'items': []
            }
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
        available_items, total_sum, manager_comment = prepare_order_items(order_data['items'])
        if not available_items:
            logger.error("No valid items after preparation")
            return {
                'success': False,
                'error': 'No valid items after preparation',
                'items': []
            }
        logger.info(f"Available items: {available_items}")
        logger.info(f"Total sum: {total_sum}")
        
        if not available_items:
            logger.error("No valid items after preparation")
            return {
                'success': False,
                'error': 'No valid items after preparation',
                'items': []
            }
        
        # Подготавливаем данные заказа
        prepared_order_data = prepare_order_data(customer_data_crm, available_items, total_sum, manager_comment, order_data['customer'].get('extra_data', ''),
                                                  order_data['customer'].get('delivery_date', ''))
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
