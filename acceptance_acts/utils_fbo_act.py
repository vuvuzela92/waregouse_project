import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timedelta
from utils_act import create_acceptance_certificate_fbo_async, load_api_tokens, get_decoded_acts, proccessing_data_acceptance_act
import base64
import io
from utils_sql import create_insert_table_db_sync


async def fbo_dict_docs(days_back: int = 4) -> dict:
    """ Получение списка документов по актам-приема передачи ФБО за указанный период """
    docs = []
    for day in range(1, days_back+1):
        beginTime = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d')
        endTime = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d')
        # ДОБАВЛЕНО AWAIT - получаем DataFrame с документами
        acceptance_certificate_df = await create_acceptance_certificate_fbo_async(beginTime, endTime)

        # Проверяем, что DataFrame не пустой
        if acceptance_certificate_df.empty:
            print("Нет документов для обработки")
            return {}
        docs.append(acceptance_certificate_df)
    # Объединяем все датафреймы в один
    all_acceptance_certificate_df = pd.concat(docs, ignore_index=True)
    # Группируем по аккаунту и получаем список документов для каждого аккаунта
    all_acceptance_certificate_dict = all_acceptance_certificate_df.groupby('account')['serviceName'].apply(list).to_dict()
    return all_acceptance_certificate_dict
    

semaphore = asyncio.Semaphore(10)

def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

async def get_decoded_acts(account, doc_list, tokens):
    batch_doc_list = list(batchify(doc_list, 50))
    url = 'https://documents-api.wildberries.ru/api/v1/documents/download/all'
    headers = {'Authorization': tokens[account]}
    acts_data = {}

    async with semaphore:
        async with aiohttp.ClientSession(headers=headers) as session:
            for batch in batch_doc_list:
                max_retries = 15
                retries = 0
                success = False

                while retries < max_retries and not success:
                    payload = {
                        "params": [
                            {"extension": "xlsx", "serviceName": doc_id}
                            for doc_id in batch
                        ]
                    }

                    try:
                        async with session.post(url, json=payload) as res:
                            print(res.status)

                            if res.status == 429:
                                retries += 1
                                print(f"429 ошибка — попытка {retries}/{max_retries}")
                                await asyncio.sleep(300)
                                continue

                            if res.status == 401:
                                print(f"401 ошибка авторизации по ЛК {account}")
                                break

                            if res.status == 400:
                                error = await res.json()
                                print(f"Ошибка 400 {account}: {error.get('message') or error}")
                                break

                            if res.status == 200:
                                data = await res.json()
                                document_data = data['data']['document']
                                decoded_data = base64.b64decode(document_data)
                                decoded_acts = io.BytesIO(decoded_data)

                                acts_data[account] = decoded_acts
                                success = True
                                retries = 0
                                break

                            print("Неожиданный статус ответа")

                    except aiohttp.ClientError as e:
                        retries += 1
                        print(f"Сетевая ошибка: {e}")
                        await asyncio.sleep(30)

    return acts_data


def create_acceptance_certificate_fbo(decoded_acts):
    """ Создание актов-приема передачи для ФБО из декодированных данных """
    docs = []
    for account, acts in decoded_acts[0].items():
        l = proccessing_data_acceptance_act(account, acts)
        for doc in l:
            docs.append(doc)
    if docs:
        df = pd.concat(docs)
    else:
        df = pd.DataFrame()

    if ' - ШК товара' in df.columns:
        # Из полученных данных формируем акты-приема передачи для ФБО
        fbo_acts_df = df[['№ п\п', 'Товар (наименование)', 'Ед. изм.', 'Фактически принято - баркод', ' - артикул продавца', ' - сорт, размер', ' - КИЗ', ' - ШК короба', ' - кол-во', 'Документ','Номер_документа', 'Дата', ' - ШК товара', 'account']]
    else:
        df[' - ШК товара'] = 0
        # Из полученных данных формируем акты-приема передачи для ФБО
        fbo_acts_df = df[['№ п\п', 'Товар (наименование)', 'Ед. изм.', 'Фактически принято - баркод', ' - артикул продавца', ' - сорт, размер', ' - КИЗ', ' - ШК короба', ' - кол-во', 'Документ','Номер_документа', 'Дата', ' - ШК товара', 'account']]


    # ВБ иногда путает акты ФБО и ФБС, поэтому фильтруем по ШК короба
    fbo_acts_df = fbo_acts_df[fbo_acts_df[' - ШК короба'].notna()]

    # Приводим названия колонок к читаемому виду
    fbo_acts_df = fbo_acts_df.rename(columns={
        '№ п\\п': 'num',
        'Товар (наименование)': 'product_name',
        'Ед. изм.': 'unit',
        'Фактически принято - баркод': 'barcode',
        ' - артикул продавца': 'vendor_code',
        ' - сорт, размер': 'size',
        ' - КИЗ': 'kiz',
        ' - ШК короба': 'box_barcode',
        ' - кол-во': 'quantity',
        'Документ': 'document',
        'Номер_документа': 'document_number',
        'Дата': 'date',
        ' - ШК товара': 'shk_id',
        'account': 'account'
    })

    # Заменяем пустоты
    fbo_acts_df['kiz'] = fbo_acts_df['kiz'].fillna('Нет КИЗов')

    # Приводим колонку с датой к нужному формату
    fbo_acts_df['date'] = fbo_acts_df['date'].str.replace('"','').str.replace(' ', '').str.replace('г.', '')
    fbo_acts_df['date'] = pd.to_datetime(fbo_acts_df['date'], format='%d%m%Y', errors='coerce')

    # Удаляем лишние символы из номера документа
    fbo_acts_df['document_number'] = fbo_acts_df['document'].str.extract(r'(\d+)\.zip')[0]
        # Обработка FBO данных
    if not fbo_acts_df.empty:
        fbo_acts_df = fbo_acts_df.where(pd.notnull(fbo_acts_df), None)  # NaN → None
        # Исправляем преобразование даты для FBO
        fbo_acts_df['date'] = pd.to_datetime(fbo_acts_df["date"], dayfirst=True, errors='coerce').dt.date
        fbo_acts_df['num'] = fbo_acts_df['num'].astype(int)
        # ВБ в какой-то момент убрал поле количество из формы акта ПП по ФБО. Поэтому меняем пустоты на единицу
        fbo_acts_df['quantity'] = fbo_acts_df['quantity'].fillna(1)
        fbo_acts_df['quantity'] = fbo_acts_df['quantity'].astype(int)
        # Поле shk_id появилось в акте позже. Поэтом предыдущие значения заполняем нулями
        fbo_acts_df['shk_id'] = fbo_acts_df['shk_id'].fillna(0)
        fbo_acts_df['shk_id'] = fbo_acts_df['shk_id'].astype(int)
        
    print('Данные по ФБО получены')
    return fbo_acts_df


async def main_fbo(days_back: int = 10):
    """   
     Основная функция для получения, обработки и записи в базу данных актов-приема передачи ФБО за указанный период.
     
     Параметры:
     days_back (int): Количество дней назад, за которые нужно получить акты. По умолчанию 5.
    """    
    # Запрашиваем список доступных документов по ФБО за указанное количество дней
    # Обработка на случай вызова из ipynb
    try:
        dict_docs_fbo = asyncio.run(fbo_dict_docs(days_back))
    except RuntimeError:
        dict_docs_fbo = asyncio.run(days_back)

    # Получаем декодированные акты ФБО асинхронно
    tasks = [asyncio.create_task(get_decoded_acts(account, doc_list, tokens=load_api_tokens())) for account, doc_list in dict_docs_fbo.items()]
    # Собираем все декодированные акты в список
    decoded_acts = await asyncio.gather(*tasks)
    # Создаем DataFrame с актами ФБО
    fbo_acts_df = create_acceptance_certificate_fbo(decoded_acts)
    # Определяем типы колонок и ключевые колонки для записи в БД
    columns_type_fbo = {
        'num': 'INTEGER',
        'product_name': 'VARCHAR(255)',
        'unit': 'VARCHAR(50)',
        'barcode': 'VARCHAR(50)', 
        'vendor_code': 'VARCHAR(50)',
        'size': 'VARCHAR(50)',
        'kiz': 'VARCHAR(255)',
        'box_barcode': 'VARCHAR(50)',
        'quantity': 'INTEGER',
        'document': 'VARCHAR(255)',
        'document_number': 'VARCHAR(50)',
        'date': 'DATE',
        'shk_id': 'BIGINT',
        'account': 'VARCHAR(100)'
    }
    # Определяем ключевые колонки для обновления записей в БД
    key_cols_fbo = ('vendor_code', 'box_barcode', 'document_number','shk_id')
    # Указываем имя таблицы для записи
    table_name_fbo = 'acceptance_fbo_acts_new'
    # Записываем данные в таблицу БД
    create_insert_table_db_sync(fbo_acts_df, table_name_fbo, columns_type_fbo, key_cols_fbo)