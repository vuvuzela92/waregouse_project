import asyncio
import aiohttp
from utils_warehouse import load_api_tokens, create_connection, create_insert_table_db_sync
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()



async def get_supplies_list(account, api_token):
    """Функция получает данные обо всех поставках по системе ФБС"""
    print(f"🟡 Начало получения поставок для {account}")
    # Список поставок
    supplies_list_api = []
    # Адрес запроса
    url = 'https://marketplace-api.wildberries.ru/api/v3/supplies'
    headers = {'Authorization': api_token}
    # Задержка при 429 ошибке
    delay = 0.3
    # Максимально количество значений за один запрос
    limit = 1000
    # Параметр пагинации
    next_page = 0
    # Количество попыток получить данные по запросу
    max_attempts = 10
    # Начальное кол-во попыток
    attempt = 0
    # Создание сессии для асинхронного http запроса
    async with aiohttp.ClientSession(headers=headers) as session:
        # Цикл из 10 попыток для получения данных
        while attempt != max_attempts:
            # Параметры запроса
            params = {'limit' : limit,
                    'next' : next_page}
            try:
                # Запуск сессии
                async with session.get(url, params=params) as res:
                    if res.status == 200:
                        # Сбрасываем счетчик при успешном запросе
                        attempt = 0
                        # Асинхронный http запрос
                        data = await res.json()
                        # Сохраняем результат запроса в переменную, в этом случае нам возвращается список
                        supplies = data['supplies']
                        # Для каждой поставки сохраняю данные об ЛК
                        for supply in supplies:
                            supply['account'] = account
                        # Добавляю расширенные данные в список
                        supplies_list_api.extend(supplies)
                        # В случае, если данные о поставках закончились или больше нет данных для дальнейшей пагинации, запросы прекращаются
                        if not supplies or data['next'] == 0:
                            break
                        # Если данные есть, то пагинация продолжается
                        else:
                            next_page = data['next']
                            # Задержка в запросе по требованию АПИ документации
                            await asyncio.sleep(delay)
                        print(f"Получены данные о {len(supplies_list_api)} поставках")
                    # Обработка неправильного запроса
                    elif res.status == 400:
                        # Создаем запрос
                        error_data = await res.json()
                        # Сохраняем в переменную. Пытаемся получить данные по ключу message. Если такого ключа нет, выведем 'Неправильный запрос'
                        error_detail = error_data.get('message', 'Неправильный запрос')
                        print(f"Ошибка 400 для аккаунта {account}: {error_detail}")
                        return None 
                    # Обработка ошибки авторизации данных 
                    elif res.status == 401:
                        print(f"Ошибка 401 для аккаунта {account}: Не авторизован")
                        return None
                    # Обработка запрета на получение данных
                    elif res.status == 403:
                        # Создаем запрос
                        error_data = await res.json()
                        # Сохраняем в переменную. Пытаемся получить данные по ключу message. Если такого ключа нет, выведем 'Неправильный запрос'
                        error_detail = error_data.get('message', 'Доступ запрещен')
                        print(f"Ошибка 403 для аккаунта {account}: {error_detail}")
                        return None
                    # Обработка лимита запросов
                    elif res.status == 429:
                        error_data = await res.json()
                        error_detail = error_data.get('detail', 'Слишком много запросов')
                        print(f"Ошибка 429 для аккаунта {account}: {error_detail}")
                        print(f"Лимит: 300 запросов в 1 минуту на аккаунт. ждем {delay}")
                        # Ждем перед повторной попыткой
                        await asyncio.sleep(delay) 
                        attempt += 1
                        continue
                    else:
                        print('Нет данных по поставкам')
                        attempt += 1
            except aiohttp.ClientError as err:
                print(f'Сетевая ошибка {err}')
            except Exception as e:
                print(f'Неожиданная ошибка {e}')
            if attempt == max_attempts:
                break
    if supplies_list_api:
        df = pd.DataFrame(supplies_list_api)
        print(f"🟢 Завершено получение поставок для {account}")
        return df
    else:
        print('Не удалось получить документы')
        return None
    
async def main():
    # Создаем задачник для получения данных о поставках по всем аккаунтам асинхронно
    tasks = [get_supplies_list(account, api_token) for account, api_token in load_api_tokens().items()]
    res = await asyncio.gather(*tasks)
    return pd.concat(res)


def get_dict_supply(days = 11, db_name = os.getenv('NAME_2'), db_user = os.getenv('USER_2'), db_password = os.getenv('PASSWORD_2'), db_host = os.getenv('HOST_2'), db_port = os.getenv('PORT_2')):
        # __________________________________________________________________________________________________________________________________________________________________#
        # Получаем список поставок
        # __________________________________________________________________________________________________________________________________________________________________#
        query = f"""
        SELECT id, account
        FROM supplies_data
        WHERE created_at > CURRENT_DATE - INTERVAL '{days} days'
        """
        # Создаем соединение с БД
        connection = create_connection(db_name, db_user, db_password, db_host, db_port)
        # Запрашиваем данные из БД
        df_db = pd.read_sql(query, connection)

        # словарь с парой аккаунт : список поставок
        df_db_dict = df_db.groupby('account')['id'].apply(list).to_dict()
        return df_db_dict


# __________________________________________________________________________________________________________________________________________________________________#
# Запрашиваем данные о содержании поставок на ВБ
# __________________________________________________________________________________________________________________________________________________________________#
async def get_orders_in_supply(account, api_token, supply_id):
    url = f'https://marketplace-api.wildberries.ru/api/v3/supplies/{supply_id}/orders'
    orders_list_api = []
    headers = {'Authorization': api_token}
    # Задержка при 429 ошибке
    delay = 0.3
    max_attempts = 10
    # Начальное кол-во попыток
    attempt = 0
    # Создание сессии для асинхронного http запроса
    async with aiohttp.ClientSession(headers=headers) as session:
        if attempt != max_attempts:
            try:
                # Запуск сессии
                async with session.get(url) as res:
                    if res.status == 200:
                        # Сбрасываем счетчик при успешном запросе
                        attempt = 0
                        # Асинхронный http запрос
                        data = await res.json()
                        # Сохраняем результат запроса в переменную, в этом случае нам возвращается список
                        orders = data['orders']
                        # Для каждой поставки сохраняю данные об ЛК
                        for order in orders:
                            order['supply_id'] = supply_id
                            order['account'] = account
                        # Добавляю расширенные данные в список
                        orders_list_api.extend(orders)
                        print(f"Получены данные о {len(orders_list_api)} заказах")
                    # Обработка неправильного запроса
                    elif res.status == 400:
                        # Создаем запрос
                        error_data = await res.json()
                        # Сохраняем в переменную. Пытаемся получить данные по ключу message. Если такого ключа нет, выведем 'Неправильный запрос'
                        error_detail = error_data.get('message', 'Неправильный запрос')
                        print(f"Ошибка 400 для аккаунта {account}: {error_detail}")
                        return None 
                    # Обработка ошибки авторизации данных 
                    elif res.status == 401:
                        print(f"Ошибка 401 для аккаунта {account}: Не авторизован")
                        return None
                    # Обработка запрета на получение данных
                    elif res.status == 403:
                        # Создаем запрос
                        error_data = await res.json()
                        # Сохраняем в переменную. Пытаемся получить данные по ключу message. Если такого ключа нет, выведем 'Неправильный запрос'
                        error_detail = error_data.get('message', 'Доступ запрещен')
                        print(f"Ошибка 403 для аккаунта {account}: {error_detail}")
                        return None
                    # Обработка лимита запросов
                    elif res.status == 429:
                        error_data = await res.json()
                        error_detail = error_data.get('detail', 'Слишком много запросов')
                        print(f"Ошибка 429 для аккаунта {account}: {error_detail}")
                        print(f"Лимит: 300 запросов в 1 минуту на аккаунт. ждем {delay}")
                        # Ждем перед повторной попыткой
                        await asyncio.sleep(delay) 
                        attempt += 1
                    else:
                        print('Нет данных по поставкам')
                        attempt += 1
            except aiohttp.ClientError as err:
                print(f'Сетевая ошибка {err}')
            except Exception as e:
                print(f'Неожиданная ошибка {e}')
    if orders_list_api:
        df = pd.DataFrame(orders_list_api)
        print(f"🟢 Завершено получение поставок для {account}")
        return df
    else:
        print('Не удалось получить документы')
        return None
    

async def fetch_supply_and_orders(dict_supply):
    tokens = load_api_tokens()
    all_orders = []
    
    # Семафор для ограничения одновременных запросов
    semaphore = asyncio.Semaphore(10)  # Максимум 10 одновременных запросов
    
    async def bounded_get_orders(account, api_token, supply_id):
        async with semaphore:
            return await get_orders_in_supply(account, api_token, supply_id)
    
    tasks = []
    
    # Создаем задачи для каждого аккаунта и каждой поставки
    for account, api_token in tokens.items():
        if account in dict_supply:
            supply_ids = dict_supply[account]
            print(f"Аккаунт {account}: обрабатываем {len(supply_ids)} поставок")
            
            for supply_id in supply_ids:
                tasks.append(bounded_get_orders(account, api_token, supply_id))
    
    # Выполняем все задачи с ограничением
    print(f"Всего задач: {len(tasks)}")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Обрабатываем результаты
    for result in results:
        if isinstance(result, Exception):
            print(f"Ошибка в задаче: {result}")
        elif isinstance(result, pd.DataFrame) and not result.empty:
            all_orders.extend(result.to_dict('records'))
    
    # Создаем DataFrame
    if all_orders:
        final_df = pd.DataFrame(all_orders)
        print(f"✅ Итого получено {len(final_df)} заказов из всех поставок")
        return final_df
    else:
        print("❌ Не удалось получить ни одного заказа")
        return pd.DataFrame()
    

async def main_get_supply_and_orders():
    dict_supply = get_dict_supply()
    final_df = await fetch_supply_and_orders(dict_supply)
    final_df = final_df.rename(columns={'scanPrice' : 'scan_price',
                                        'orderUid' : 'order_uid',
                                        'colorCode' : 'color_code',
                                        'createdAt' : 'created_at',
                                        'warehouseId' : 'warehouse_id',
                                        'nmId' : 'nm_id',
                                        'chrtId' : 'chrt_id',
                                        'convertedPrice' : 'converted_price',
                                        'currencyCode' : 'currency_code',
                                        'convertedCurrencyCode' : 'converted_currency_code',
                                        'cargoType' : 'cargo_type',
                                        'isZeroOrder' : 'is_zero_order'})

    pattern = r'(wild\d+)'
    # Используем метод str.extract для извлечения нужного паттерна
    final_df['local_vendor_code'] = final_df['article'].str.extract(pattern)
    # Обрабатываем колонки, убирая лишние символы
    final_df['offices'] = final_df['offices'].astype(str)
    final_df['offices'] = final_df['offices'].str.replace('[', '').str.replace(']', '')
    # Убираем скобки
    final_df['skus'] = final_df['skus'].astype(str)
    final_df['skus'] = final_df['skus'].str.replace('[', '').str.replace(']', '')
    # Убираем фигурные скобки
    final_df['options'] = final_df['options'].astype(str)
    final_df['options'] = final_df['options'].str.replace('{', '').str.replace('}', '')
    # Приводим цены к реальному виду
    final_df['price'] = final_df['price']/100
    final_df['converted_price'] = final_df['converted_price']/100
    final_df = final_df[['supply_id' ,'scan_price', 'order_uid', 'local_vendor_code', 'article', 'color_code', 'rid', 'created_at',
        'offices', 'skus', 'id', 'warehouse_id', 'nm_id', 'chrt_id', 'price',
        'converted_price', 'currency_code', 'converted_currency_code',
        'cargo_type', 'is_zero_order', 'options', 'account',
        ]]

    table_name = 'supplies_and_orders'
    columns_type = {'supply_id' : 'VARCHAR(255)',
                    'scan_price' : 'NUMERIC',
                    'order_uid' : ' VARCHAR(255)',
                    'local_vendor_code' : 'VARCHAR(255)',
                    'article' : 'VARCHAR(255)',
                    'color_code' : 'VARCHAR(255)',
                    'rid' : 'VARCHAR(255)',
                    'created_at' : 'TIMESTAMP',
                    'offices' : 'VARCHAR(255)',
                    'skus' : 'VARCHAR(255)',
                    'id' : 'BIGINT',
                    'warehouse_id' : 'INTEGER',
                    'nm_id' : 'BIGINT',
                    'chrt_id' : 'BIGINT',
                    'price' : 'NUMERIC(10,2)',
                    'converted_price' : 'NUMERIC(10,2)', 
                    'currency_code' : 'INTEGER',
                    'converted_currency_code' : 'INTEGER',
                    'cargo_type' : 'INTEGER',
                    'is_zero_order' : 'BOOLEAN',
                    'options' : 'VARCHAR(255)',
                    'account' : 'VARCHAR(255)',
                    }

    # Ключевые колонки для UPSERT
    key_columns = ('supply_id', 'id')
    create_insert_table_db_sync(final_df, table_name, columns_type, key_columns)