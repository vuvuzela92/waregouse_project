"""Функции для работы с БД"""
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import os
import asyncio
import pandas as pd
from datetime import datetime
import asyncio
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging


load_dotenv()

# Подключение к базе данных
def create_connection_to_vector_db():
    user = os.getenv('USER_2')
    password = os.getenv('PASSWORD_2')
    database = os.getenv('NAME_2') 
    host = os.getenv('HOST_2')
    port = os.getenv('PORT_2')
    connection = None
    try:
        connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        print(f"Соединение с БД PostgreSQL успешно установлено в {datetime.now().strftime('%Y-%m-%d-%H:M')}")
    except OperationalError as error:
        print(f"Произошла ошибка при подключении к БД PostgreSQL {error}")
    return connection

# Исполнение SQL запросов
def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()  # явное подтверждение транзакции
        print(f"Запрос успешно выполнен в {datetime.now().strftime('%Y-%m-%d')}")
    except Exception as e:
        connection.rollback()  # откат транзакции в случае ошибки
        print(f"Ошибка выполнения запроса: {e}")
    finally:
        cursor.close()

# Функция на чтение данных из БД
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as error:
        print(f'Произошла ошибка при выводе данных {error}')

# Функция для получения датафрейма из БД
def get_db_table(db_query: str, connection):
    """Функция получает данные из Базы Данных и преобразует их в датафрейм"""
    execute_read_query(connection, db_query)
    # Преобразуем таблицу в датафрейм
    try:
        df_db = pd.read_sql(db_query, connection).fillna(0).infer_objects(copy=False)
        print('Данные из БД загружены в датафрейм')
        return df_db
    except Exception as e:

        print(f'Ошибка получения данных из БД {e}')

# Глобальный лок для создания таблицы
table_creation_lock = asyncio.Lock()
def create_insert_table_db_sync(df: pd.DataFrame, table_name: str, columns_type: dict, key_columns: tuple):
    load_dotenv()
    
    user = os.getenv('USER_2')
    password = os.getenv('PASSWORD_2')
    database = os.getenv('NAME_2') 
    host = os.getenv('HOST_2')
    port = os.getenv('PORT_2')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = None
    
    try:
        engine = create_engine(connection_string)
        
        # Проверка типов данных
        valid_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'NUMERIC', 'DATE', 'TIMESTAMP', 'BOOLEAN', 'TEXT', 'VARCHAR']
        for col, dtype in columns_type.items():
            if not dtype.strip():
                raise ValueError(f"Пустой тип данных для колонки {col}")
            base_type = dtype.split('(')[0].strip().upper()
            if base_type not in valid_types:
                raise ValueError(f"Недопустимый тип данных для колонки {col}: {dtype}")

        # Проверка и добавление отсутствующих колонок
        missing_cols = set(columns_type.keys()) - set(df.columns)
        for col in missing_cols:
            df[col] = None
            logging.info(f"Добавлена отсутствующая колонка {col} с None")
        
        extra_cols = set(df.columns) - set(columns_type.keys())
        if extra_cols:
            logging.warning(f"Лишние колонки в DataFrame: {extra_cols}, они будут проигнорированы")

        # Формирование SQL для создания таблицы
        columns_definition = ", ".join([f"{col} {dtype}" for col, dtype in columns_type.items()])
        unique_constraint = f", CONSTRAINT unique_{table_name} UNIQUE ({', '.join(key_columns)})" if key_columns else ""

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_definition}{unique_constraint}
            )
        """

        # Создание таблицы
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        
        # Подготовка данных для вставки
        columns = list(columns_type.keys())
        
        # Создание временной таблицы для UPSERT
        temp_table = f"temp_{table_name}"
        
        # Вставляем данные во временную таблицу
        df[columns].to_sql(temp_table, engine, if_exists='replace', index=False)
        
        # Выполняем UPSERT из временной таблицы с явным преобразованием типов
        with engine.connect() as conn:
            if key_columns:
                # Формируем SELECT с явным преобразованием типов
                select_columns = []
                for col in columns:
                    if columns_type[col].upper() == 'TIMESTAMP':
                        select_columns.append(f"CAST({col} AS TIMESTAMP)")  # Явное преобразование
                    else:
                        select_columns.append(col)
                
                updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in key_columns])
                upsert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    SELECT {', '.join(select_columns)} FROM {temp_table}
                    ON CONFLICT ({', '.join(key_columns)}) 
                    DO UPDATE SET {updates}
                """
            else:
                # Простая вставка с преобразованием типов
                select_columns = []
                for col in columns:
                    if columns_type[col].upper() == 'TIMESTAMP':
                        select_columns.append(f"CAST({col} AS TIMESTAMP)")
                    else:
                        select_columns.append(col)
                
                upsert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    SELECT {', '.join(select_columns)} FROM {temp_table}
                """
            
            conn.execute(text(upsert_query))
            # Удаляем временную таблицу
            conn.execute(text(f"DROP TABLE {temp_table}"))
            conn.commit()
        
        logging.info(f"Успешно сохранено {len(df)} строк в {table_name}")
        
    except SQLAlchemyError as e:
        logging.error(f"Ошибка при работе с БД: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Неожиданная ошибка: {str(e)}")
        raise
    finally:
        if engine:
            engine.dispose()    