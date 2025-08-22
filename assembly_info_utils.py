import asyncio
import aiohttp
import pandas as pd
import sys
import os
# Укажи путь к папке, где лежит utils_warehouse.py
sys.path.append(r'D:\Pytnon_scripts\warehouse_scripts')
sys.path.append(r'D:\Pytnon_scripts\tokens.json')
from utils_warehouse import load_api_tokens
from time import time
import logging

logging.basicConfig(
    filename='assembly_info_log.log',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

semaphore = asyncio.Semaphore(8)  # максимум 8 одновременных запросов

async def fetch_wb_assembly_task_info(account: str, api_token: str):
        """ Функция получает информацию обо всех сборочных заданиях, крому их статуса.
        Получаем данные за последние 30 дней.

        Параметры:
            account - название аккаунта на ВБ
            api_token - апи-ключ ЛК
        """
        async with semaphore:
            url = 'https://marketplace-api.wildberries.ru/api/v3/orders'
            headers = {'Authorization': api_token}

            full_data = []
            next_cursor = 0
            max_attempts = 3

            # Создаём сессию один раз
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    params = {
                        'limit': 1000,
                        'next': next_cursor
                    }

                    success = False
                    for attempt in range(max_attempts):
                        try:
                            async with session.get(url, headers=headers, params=params) as res:
                                logger.info(f'Получили статус запроса {res.status}')
                                # Проверяем статус
                                if res.status == 200:
                                    data = await res.json()
                                    orders = data.get('orders', [])
                                    for order in orders:
                                        order['account'] = account
                                    full_data.extend(orders)
                                    print(f'Получены данные по кабинету {account}')

                                    # Обновляем курсор
                                    next_cursor = data.get('next', 0)
                                    success = True
                                    break  # Успешно получили данные

                                elif res.status == 401:
                                    try:
                                        error_detail = await res.json()  # Пробуем получить JSON
                                    except Exception as e:
                                        error_detail = await res.text()  # Если не JSON — хотя бы текст

                                    logger.error(
                                        f"[{account}] Ошибка авторизации 401. "
                                        f"Ответ сервера: {error_detail}"
                                    )
                                    return full_data  # Дальше нет смысла

                                elif res.status == 400:
                                    try:
                                        error_detail = await res.json()  # Пробуем получить JSON
                                    except Exception as e:
                                        error_detail = await res.text()  # Если не JSON — хотя бы текст
                                    logger.error(f"[{account}] Ошибка запроса: 400. Проверьте параметры.")
                                    print(f"[{account}] Ошибка запроса: 400. Проверьте параметры.")
                                    return full_data

                                elif res.status == 429:
                                    logger.error(f"[{account}] Слишком много запросов. Ждём 60 секунд...")
                                    print(f"[{account}] Слишком много запросов. Ждём 60 секунд...")
                                    await asyncio.sleep(60)
                                    continue  # Повторим попытку

                                elif 400 <= res.status < 500:
                                    logger.error(f"[{account}] Клиентская ошибка: {res.status}")
                                    print(f"[{account}] Клиентская ошибка: {res.status}")
                                    break  # Не повторяем

                                else:
                                    logger.error(f"[{account}] Серверная ошибка: {res.status}. Попытка {attempt + 1}")
                                    print(f"[{account}] Серверная ошибка: {res.status}. Попытка {attempt + 1}")
                                    await asyncio.sleep(1)  # Ждём перед повтором

                        except aiohttp.ClientConnectorError as e:
                            print(f"[{account}] Ошибка подключения: {e}")
                        except aiohttp.ServerDisconnectedError:
                            print(f"[{account}] Сервер разорвал соединение")
                        except aiohttp.ClientTimeout:
                            print(f"[{account}] Таймаут соединения")
                        except asyncio.TimeoutError:
                            print(f"[{account}] Таймаут asyncio")
                        except Exception as e:
                            print(f"[{account}] Неизвестная ошибка: {e}")

                        # Задержка перед повтором
                        await asyncio.sleep(1)

                    if not success:
                        print(f"[{account}] Не удалось получить данные после {max_attempts} попыток. Прерываем.")
                        break

                    # Если next == 0 — больше нет данных
                    if next_cursor == 0:
                        break

                    # Соблюдаем лимит: 200 мс между запросами
                    await asyncio.sleep(0.2)

            return full_data

        
async def fetch_all_assembly_data():
    """ Функция получает информацию по всем кабинетам асинхронно
    и возвращает список списков с информацие о сборочных заданиях
    по каждому кабинету."""
    tasks = [fetch_wb_assembly_task_info(account, token)
            for account, token in load_api_tokens().items()]
    results = await asyncio.gather(*tasks)
    return results


def create_assembly_info_df(all_data: list):
    """ Функция принимает список списков с информацие о сборочных заданиях и возвращает 
    единый датафрейм с краткой информацией по каждому."""
    df = pd.DataFrame([order for account_orders in all_data for order in account_orders])
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True)
    # Переводим в MSK (UTC+3)
    df['createdAt_msk'] = df['createdAt'].dt.tz_convert('Europe/Moscow')
    # df_assembly_id = df[['createdAt_msk', 'id', 'nmId', 'article', 'supplyId', 'account']]
    # Регулярное выражение для извлечения нужной части
    pattern = r'(wild\d+)'
    # Используем метод str.extract для извлечения нужного паттерна
    # df_assembly_id['local_vendor_code'] = df_assembly_id['article'].str.extract(pattern)
    df['local_vendor_code'] = df['article'].str.extract(pattern)
    return df

async def get_tasks_status(account: str, api_token: str, payload: dict):
    """
    Получает статусы сборочных заданий по их ID через API Wildberries.

    Args:
        account (str): Название аккаунта (для метки).
        api_token (str): API-токен.
        payload (dict): Словарь вида {"orders": [123, 456, ...]}, макс. 1000 ID.

    Returns:
        list[dict]: Список заказов с полями 'orderId', 'supplierStatus', 'wbStatus', и 'account'.
    """
    url = 'https://marketplace-api.wildberries.ru/api/v3/orders/status'
    headers = {'Authorization': api_token}
    full_data = []
    max_attempts = 5

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(max_attempts):
            try:
                async with session.post(url, headers=headers, json=payload) as res:
                    # Только при 200 пытаемся парсить JSON
                    if res.status == 200:
                        data = await res.json()
                        orders = data.get('orders', [])
                        for order in orders:
                            order['account'] = account
                        full_data.extend(orders)
                        print(f"[{account}] Успешно получены статусы для {len(orders)} заказов")
                        return full_data 

                    elif res.status == 401:
                        print(f"[{account}] Ошибка авторизации: 401. Проверьте токен.")
                        return full_data

                    elif res.status == 400:
                        print(f"[{account}] Ошибка запроса: 400. Проверьте payload (формат: {{'orders': [...]}}).")
                        return full_data

                    elif res.status == 429:
                        print(f"[{account}] Слишком много запросов. Ждём 60 секунд...")
                        await asyncio.sleep(60)
                        continue  # Повторим попытку

                    elif 400 <= res.status < 500:
                        print(f"[{account}] Клиентская ошибка: {res.status}")
                        return full_data  # Не повторяем

                    else:
                        print(f"[{account}] Серверная ошибка: {res.status}. Попытка {attempt + 1} из {max_attempts}")
                        await asyncio.sleep(1)

            except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as e:
                print(f"[{account}] Ошибка подключения: {e}")
            except aiohttp.ClientTimeout:
                print(f"[{account}] Таймаут соединения")
            except asyncio.TimeoutError:
                print(f"[{account}] Таймаут asyncio")
            except Exception as e:
                print(f"[{account}] Неизвестная ошибка: {e}")

            # Задержка перед повторной попыткой
            await asyncio.sleep(1)

        # Если все попытки провалились
        print(f"[{account}] Не удалось получить данные после {max_attempts} попыток.")
        return full_data
    

def chunked(lst, n):
    """Разбивает список на части по n элементов."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def fetch_all_statuses(assembly_dict, tokens_dict, max_concurrent=8):
    """
    Для каждого аккаунта получает статусы сборочных заданий.

    Args:
        assembly_dict (dict): {account: [orderId, ...]}
        tokens_dict (dict): {account: api_token}
        max_concurrent (int): Максимум одновременных запросов

    Returns:
        pd.DataFrame: Все статусы со всеми аккаунтами
    """
    semaphore = asyncio.Semaphore(max_concurrent)  # Ограничиваем параллельность
    all_statuses = []

    async def fetch_for_account(account, token, order_ids):
        async with semaphore:
            for chunk in chunked(order_ids, 1000):  # по 1000 ID
                payload = {"orders": chunk}
                try:
                    statuses = await get_tasks_status(account, token, payload)
                    if statuses:
                        all_statuses.extend(statuses)
                except Exception as e:
                    print(f"[{account}] Ошибка при получении статусов: {e}")
                # Лимит: 200 мс между запросами
                await asyncio.sleep(0.2)

    # Создаём задачи для всех аккаунтов
    tasks = []
    for account, token in tokens_dict.items():
        order_ids = assembly_dict.get(account, [])
        if order_ids:
            task = fetch_for_account(account, token, order_ids)
            tasks.append(task)
        else:
            print(f"[{account}] Нет ID для получения статусов")

    # Запускаем все задачи
    await asyncio.gather(*tasks)

    # Возвращаем DataFrame
    return pd.DataFrame(all_statuses)