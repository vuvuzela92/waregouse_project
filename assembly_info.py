from assembly_info_utils import fetch_all_assembly_data, create_assembly_info_df, fetch_all_statuses
import pandas as pd
import sys
# Укажи путь к папке, где лежит utils_warehouse.py
# sys.path.append(r'D:\Pytnon_scripts\warehouse_scripts')
# sys.path.append(r'D:\Pytnon_scripts\tokens.json')
from utils_warehouse import load_api_tokens, create_insert_table_db
import asyncio
import logging


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Определяем директорию текущего файла (скрипта)
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assembly_info_log.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info('Начало работы скрипта')
    # Получаем данные по сборочным заданиям
    all_data = asyncio.run(fetch_all_assembly_data())
    # Создаем датафрейм с информацие по сборочным заданиям
    df_assembly_id = create_assembly_info_df(all_data)
    # Создаем словарь для запроса статусов сборочных заданий
    assembly_dict = df_assembly_id.groupby('account')['id'].apply(list).to_dict()
    # Получаем информацию по сборочным заданиям в даатфрейме
    df_statuses = asyncio.run(fetch_all_statuses(assembly_dict, load_api_tokens()))
    # Объединяем датафреймы по сборочным заданиям и их статусам
    df_status_model = pd.merge(df_assembly_id, df_statuses, how='left', on=['id', 'account'])

    # Переименуем для корректного перемещения в БД
    df_status_model = df_status_model.rename(columns={'scanPrice' : 'scan_price',
                                                    'deliveryType' : 'delivery_type',
                                                    'orderUid' : 'order_uid',
                                                    'colorCode' : 'color_code',
                                                    'createdAt' : 'created_at',
                                                    'warehouseId' : 'warehouse_id',
                                                    'chrtId' : 'chrt_id',
                                                    'convertedPrice' : 'converted_price',
                                                    'currencyCode' : 'currency_code',
                                                    'convertedCurrencyCode' : 'converted_currency_code',
                                                    'cargoType' : 'cargo_type',
                                                    'isZeroOrder' : 'is_zero_order',
                                                    'officeId' : 'office_id',
                                                    'createdAt_msk' : 'created_at_msk',
                                                    'nmId': 'nm_id', 
                                                    'article' : 'vendor_code',
                                                    'supplyId' : 'supply_id',
                                                    'supplierStatus' : 'supplier_status',
                                                    'wbStatus' : 'wb_status'})

    df_status_model['date'] = pd.to_datetime(df_status_model['created_at_msk']).dt.date

    df_status_model = df_status_model[['date', 'nm_id', 'local_vendor_code', 'vendor_code', 'id', 'supplier_status', 'wb_status',
                                        'supply_id', 'address', 'scan_price', 'price', 'converted_price', 'comment', 'delivery_type',
                                        'order_uid', 'color_code', 'rid', 'created_at', 'created_at_msk', 'offices', 'skus',
                                        'warehouse_id', 'chrt_id', 'currency_code', 'converted_currency_code', 'cargo_type', 'is_zero_order',
                                        'options', 'office_id', 'account']]
    # Обрабатываем колонки, убирая лишние символы
    df_status_model['offices'] = df_status_model['offices'].astype(str)
    df_status_model['offices'] = df_status_model['offices'].str.replace('[', '').str.replace(']', '')
    # Убираем скобки
    df_status_model['skus'] = df_status_model['skus'].astype(str)
    df_status_model['skus'] = df_status_model['skus'].str.replace('[', '').str.replace(']', '')
    # Убираем фигурные скобки
    df_status_model['options'] = df_status_model['options'].astype(str)
    df_status_model['options'] = df_status_model['options'].str.replace('{', '').str.replace('}', '')
    # Приводим цены к реальному виду
    df_status_model['price'] = df_status_model['price']/100
    df_status_model['converted_price'] = df_status_model['converted_price']/100

    # На всякий случай, проверяем наличие дубликатов
    if df_status_model[['id', 'supplier_status', 'wb_status']].duplicated().sum() > 0:
        df_status_model = df_status_model.drop_duplicates(subset=['id', 'supplier_status', 'wb_status'])
    # Устанавливаем типы данных для БД
    columns_type = {
        'date': 'DATE',  # или 'DATE', если это дата в формате '2025-08-21'
        'nm_id': 'BIGINT',
        'local_vendor_code': 'TEXT',
        'vendor_code': 'TEXT',
        'id': 'BIGINT',
        'supplier_status': 'TEXT',
        'wb_status': 'TEXT',
        'supply_id': 'TEXT',
        'address': 'TEXT',
        'scan_price': 'NUMERIC(10,2)',
        'price': 'INTEGER',
        'converted_price': 'INTEGER',
        'comment': 'TEXT',
        'delivery_type': 'TEXT',
        'order_uid': 'TEXT',
        'color_code': 'TEXT',
        'rid': 'TEXT',
        'created_at': 'TIMESTAMPTZ',  # TIMESTAMP WITH TIME ZONE
        'created_at_msk': 'TIMESTAMPTZ',
        'offices': 'TEXT',
        'skus': 'TEXT',
        'warehouse_id': 'INTEGER',
        'chrt_id': 'BIGINT',
        'currency_code': 'INTEGER',
        'converted_currency_code': 'INTEGER',
        'cargo_type': 'INTEGER',
        'is_zero_order': 'BOOLEAN',
        'options': 'TEXT',
        'office_id': 'INTEGER',
        'account': 'VARCHAR(50)'
    }
    # Задаем имя таблицы
    table_name = 'assembly_task_status_model'
    # Задаем уникальные поля для проверки уникальности данных
    key_cols = ('id', 'supplier_status', 'wb_status')
    # Отправляем данные в таблицу БД
    create_insert_table_db(df_status_model, table_name, columns_type, key_cols)
