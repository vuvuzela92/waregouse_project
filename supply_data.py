import asyncio
from supply_data_utils import main
from utils_warehouse import create_insert_table_db_sync

if __name__ == '__main__':

    final_df = asyncio.run(main())
    # Подготовим данные для вставки в БД
    final_df = final_df.rename(columns={'closedAt': 'closed_at',
                                        'scanDt': 'scan_dt',
                                        'rejectDt': 'reject_dt',
                                        'destinationOfficeId': 'destination_office_id',
                                        'createdAt': 'created_at',
                                        'cargoType': 'cargo_type'})
    table_name = 'supplies_data'
    columns_type = {
        'closed_at': 'TIMESTAMP',
        'scan_dt': 'TIMESTAMP', 
        'reject_dt': 'TIMESTAMP',
        'destination_office_id': 'INTEGER',
        'id': 'VARCHAR(255)',  # или VARCHAR(255)
        'name': 'TEXT',
        'created_at': 'TIMESTAMP',
        'cargo_type': 'INTEGER',
        'done': 'BOOLEAN',
        'account': 'VARCHAR(255)',
        'created_at_db': 'TIMESTAMP'
    }

    # Ключевые колонки для UPSERT
    key_columns = ('id',)  # id как первичный ключ
    create_insert_table_db_sync(final_df, table_name, columns_type, key_columns)