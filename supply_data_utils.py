import asyncio
import aiohttp
from utils_warehouse import load_api_tokens
import pandas as pd


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
    # Создаем задачник для получения данных по всем аккаунтам асинхронно
    tasks = [get_supplies_list(account, api_token) for account, api_token in load_api_tokens().items()]
    res = await asyncio.gather(*tasks)
    return pd.concat(res)