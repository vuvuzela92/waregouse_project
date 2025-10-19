import pandas as pd
from supply_data_utils import fetch_supply_and_orders
import asyncio

if __name__ == '__main__':
    asyncio.run(fetch_supply_and_orders())