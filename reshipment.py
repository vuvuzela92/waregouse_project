import asyncio
import pandas as pd
from utils_warehouse import load_api_tokens, re_shipment_info_get, create_insert_table_db_async
from datetime import datetime



today = (datetime.now()).strftime('%Y-%m-%d')
async def main():
    tasks = []
    for account, api_token in load_api_tokens().items():
        task = re_shipment_info_get(account, api_token, today)
        tasks.append(task)    
    all_data = await asyncio.gather(*tasks)
    processed_data = []
    for i in all_data:
        orders = i['orders']
        for order in orders:
            if order and order != []:  # Проверяем, что order не пустой
                processed_data.append(order)

        # Создаем один DataFrame из всех данных
        if processed_data:
            df_orders = pd.DataFrame(processed_data)
        else:
            print("Нет данных для создания DataFrame")
        df_orders = df_orders.rename(columns={'supplyId' : 'supply_id',
                                              'orderId' : 'order_id'})
        
    return df_orders

if __name__ == "__main__":
    df = asyncio.run(main())

    columns_type = {'supply_id' : 'TEXT',
                    'order_id' : 'BIGINT',
                    'date' : 'DATE',
                    'account' : 'TEXT'}
    key_columns = ('order_id', 'account')
    # Перед вставкой в БД нужно привести столбец date в тип datetime.date
    df['date'] = pd.to_datetime(df['date']).dt.date
    table_name = 're_shipments'
    asyncio.run(create_insert_table_db_async(df, table_name, columns_type, key_columns))