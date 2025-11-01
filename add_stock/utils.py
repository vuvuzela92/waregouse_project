import asyncio
from aiohttp import ClientSession
import asyncpg
from typing import List, Dict, Any
import gspread
from time import time



class OrderStatusRepository:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    async def get_order_status_counts_by_vendor(self) -> List[Dict[str, Any]]:
        """
        Получает количество заказов по статусам для каждого вендора
        """
        query = """
        SELECT 
            a.local_vendor_code,
            COUNT(CASE WHEN osl.status = 'NEW' THEN 1 END) as new_count
        FROM public.order_status_log osl
        JOIN assembly_task as ast ON ast.task_id = osl.order_id
        JOIN article as a ON ast.article_id = a.nm_id
        INNER JOIN (
            SELECT 
                order_id, 
                MAX(created_at) as max_created_at
            FROM public.order_status_log
            GROUP BY order_id
        ) latest ON osl.order_id = latest.order_id AND osl.created_at = latest.max_created_at
        WHERE osl.status IN ('IN_TECHNICAL_SUPPLY', 'NEW')
        GROUP BY a.local_vendor_code
        ORDER BY new_count DESC;
        """

        async with asyncpg.create_pool(self.connection_string) as pool:
            async with pool.acquire() as connection:
                rows = await connection.fetch(query)
                return [dict(row) for row in rows]





def insert_multiple_columns(sheet, headers_loc: int, headers: list, data_loc: int, data_matrix: list):
    """
    Вставляет сразу несколько колонок в таблицу.
    
    :param sheet: Лист таблицы
    :param headers_loc: Номер строки заголовков
    :param headers: Список заголовков колонок, которые нужно обновить
    :param data_loc: Строка начала вставки (например, 2)
    :param data_matrix: Список списков со значениями по колонкам. Формат:
                        [
                            [val1_col1, val1_col2, val1_col3],
                            [val2_col1, val2_col2, val2_col3],
                            ...
                        ]
    """
    sheet_headers = sheet.row_values(headers_loc)
    
    col_indices = []
    for header in headers:
        try:
            col_index = sheet_headers.index(header) + 1
            col_indices.append(col_index)
        except ValueError:
            raise ValueError(f"Заголовок {header} не найден")
    
    for i, col_index in enumerate(col_indices):
        # Формируем данные по конкретной колонке
        col_data = [[row[i]] for row in data_matrix]
        
        start_cell = rowcol_to_a1(row=data_loc, col=col_index)
        end_cell = rowcol_to_a1(row=data_loc + len(data_matrix) - 1, col=col_index)
        
        cell_range = f"{start_cell}:{end_cell}"
        sheet.update(cell_range, col_data)
        print(f'Данные {headers} обновлены')


def insert_wild_data_correct(sheet, data_dict: dict, sheet_header="wild") -> None:
        """
        Оптимизированная версия - обновляет данные целыми столбцами.
        """

        
        def get_column_letter(col_idx: int) -> str:
            """Конвертирует индекс колонки в букву (A, B, C, ...)"""
            result = ""
            while col_idx > 0:
                col_idx, remainder = divmod(col_idx - 1, 26)
                result = chr(65 + remainder) + result
            return result
        
        try:
            # Получаем заголовки таблицы
            headers = sheet.row_values(1)
            print(headers)
            # Находим индекс колонки wild
            wild_col_idx = None
            for idx, header in enumerate(headers):
                if sheet_header in header.lower():
                    wild_col_idx = idx
                    print(wild_col_idx)

            if wild_col_idx is None:

                print(f"Колонка {sheet_header} не найдена в таблице")
                return

            # Находим индексы и диапазон наших целевых колонок
            target_headers = list(next(iter(data_dict.values())).keys()) if data_dict else []
            target_indices = []

            for header in target_headers:
                if header in headers:
                    target_indices.append(headers.index(header))

            if not target_indices:
                print("Целевые заголовки не найдены в таблице")
                return

            # Сортируем индексы и проверяем, что они идут подряд
            target_indices.sort()
            is_consecutive = all(target_indices[i] + 1 == target_indices[i + 1]
                                 for i in range(len(target_indices) - 1))

            # Получаем все данные таблицы
            all_data = sheet.get_all_values()

            # Создаем матрицу для обновления (строки x колонки)
            updates = []

            if is_consecutive and len(target_indices) > 1:
                # ОПТИМИЗАЦИЯ: обновляем целым диапазоном столбцов
                start_col = target_indices[0]
                end_col = target_indices[-1]

                # ПРАВИЛЬНО формируем диапазон: "AX2:BA5886"
                start_col_letter = get_column_letter(start_col + 1)
                end_col_letter = get_column_letter(end_col + 1)
                update_range = f"{start_col_letter}2:{end_col_letter}{len(all_data)}"

                print(f"Обновляем диапазон: {update_range}")

                # Создаем матрицу обновлений
                update_matrix = [['' for _ in range(len(target_indices))] for _ in range(len(all_data) - 1)]

                # Заполняем матрицу данными
                for row_idx in range(1, len(all_data)):
                    row = all_data[row_idx]
                    if len(row) > wild_col_idx:
                        current_wild = row[wild_col_idx]
                        if current_wild in data_dict:
                            wild_data = data_dict[current_wild]
                            for i, col_idx in enumerate(target_indices):
                                header = headers[col_idx]
                                if header in wild_data:
                                    update_matrix[row_idx - 1][i] = wild_data[header]
                                else:
                                    # Сохраняем оригинальное значение если нет в словаре
                                    update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                        else:
                            # Сохраняем оригинальные значения для строк без совпадения
                            for i, col_idx in enumerate(target_indices):
                                update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''
                    else:
                        # Для строк без wild данных
                        for i, col_idx in enumerate(target_indices):
                            update_matrix[row_idx - 1][i] = row[col_idx] if col_idx < len(row) else ''

                updates.append({
                    'range': update_range,
                    'values': update_matrix
                })
            else:
                # Если колонки не подряд, обновляем каждую колонку отдельно
                for col_idx in target_indices:
                    header = headers[col_idx]
                    col_letter = get_column_letter(col_idx + 1)
                    # ПРАВИЛЬНЫЙ формат: "AX2:AX5886"
                    col_range = f"{col_letter}2:{col_letter}{len(all_data)}"

                    print(f"Обновляем колонку: {col_range}")

                    # Подготавливаем данные для столбца
                    column_data = []
                    for row_idx in range(1, len(all_data)):
                        row = all_data[row_idx]
                        if len(row) > wild_col_idx:
                            current_wild = row[wild_col_idx]
                            if current_wild in data_dict and header in data_dict[current_wild]:
                                column_data.append([data_dict[current_wild][header]])
                            else:
                                column_data.append([row[col_idx] if col_idx < len(row) else ''])
                        else:
                            column_data.append([row[col_idx] if col_idx < len(row) else ''])

                    updates.append({
                        'range': col_range,
                        'values': column_data
                    })

            # pprint(updates)

            # Выполняем обновление
            if updates:
                for i, update in enumerate(updates):
                    try:
                        sheet.update(update['range'], update['values'], value_input_option='RAW')
                        print(f"Успешно обновлен диапазон {update['range']} ({i + 1}/{len(updates)})")
                    except Exception as e:
                        print(f"Ошибка при обновлении {update['range']}: {e}")
                        # Можно добавить повторные попытки или продолжить

        except Exception as e:
            print(f"Ошибка при вставке данных: {e}")
            raise

async def get_stock_data_on_api():
    connection_string = "postgresql://vector_admin:skurbick01052023@149.154.66.213/vector_db"
    repo = OrderStatusRepository(connection_string)
    db_data =  await repo.get_order_status_counts_by_vendor()
    return_data = {}
    url_stock = "http://149.154.66.213:8302/api/warehouse_and_balances/get_all_product_current_balances"
    url_reserve = "http://149.154.66.213:8302/api/shipment_of_goods/summ_reserve_data"

    async with ClientSession() as session:
        async with session.get(url=url_reserve) as response:
            reserve_response_json = await response.json()
            for res in reserve_response_json:
                product_id = res['product_id']
                if product_id not in return_data:
                    return_data[product_id] = {
                        "Физ остаток\n(сервис)": 0,
                        "Свободный остаток\n(сервис)": 0,
                        "Резерв ФБС\n(сервис)": 0,
                        "Резерв ФБО\n(сервис)": 0
                    }
                for delivery_type_data in res['delivery_type_data']:
                    reserve_type = delivery_type_data['reserve_type']
                    current_reserve = delivery_type_data['current_reserve']
                    if reserve_type == "ФБО":
                        return_data[product_id]["Резерв ФБО\n(сервис)"] += current_reserve
                    if reserve_type == "ФБС":
                        return_data[product_id]["Резерв ФБС\n(сервис)"] += current_reserve

    async with ClientSession() as session:
        async with session.get(url=url_stock) as response:
            stock_response_json = await response.json()
            for res in stock_response_json:
                if res['warehouse_id'] != 1:
                    continue
                product_id = res['product_id']
                if product_id not in return_data:
                    return_data[product_id] = {
                        "Физ остаток\n(сервис)": 0,
                        "Свободный остаток\n(сервис)": 0,
                        "Резерв ФБС\n(сервис)": 0,
                        "Резерв ФБО\n(сервис)": 0
                    }
                physical_quantity = res["physical_quantity"]
                available_quantity = res["available_quantity"]
                return_data[product_id]["Физ остаток\n(сервис)"] += physical_quantity
                return_data[product_id]["Свободный остаток\n(сервис)"] += available_quantity
    for res in db_data:
        product_id = res['local_vendor_code']
        if product_id not in return_data:
            return_data[product_id] = {
                "Физ остаток\n(сервис)": 0,
                "Свободный остаток\n(сервис)": 0,
                "Резерв ФБС\n(сервис)": 0,
                "Резерв ФБО\n(сервис)": 0
            }
        return_data[product_id]["Резерв ФБС\n(сервис)"] += res['new_count']
        return_data[product_id]["Свободный остаток\n(сервис)"] -= res['new_count']
    return return_data

def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename='creds.json')
    for attempt in range(1, retries + 1):
        print(f"[Попытка {attempt} октрыть доступ к таблице")
        try:
            return gc.open(title)
        except gspread.error as e:
            if "503" in str(e):
                print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                time.sleep(delay)
            else:
                raise  # если ошибка не 503 — пробрасываем дальше
    raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")


# -------------------------------Финальная функция для отправки данных в Расчет закупки New-----------------------------

def send_stock_to_gs():
    res = asyncio.run(get_stock_data_on_api())
    # Открывает доступ к гугл-таблице
    table_name = 'Расчет закупки NEW'
    sheet_name = 'Остатки из сервиса'
    table = safe_open_spreadsheet(table_name)
    sheet = table.worksheet(sheet_name)
    insert_wild_data_correct(sheet, res, sheet_header="wild")