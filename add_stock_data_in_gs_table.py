import schedule
import time
from utils import send_stock_to_gs


def job():
    print("Запуск обновления остатков в Google Sheets...")
    try:
        send_stock_to_gs()
        print("Обновление успешно завершено.")
    except Exception as e:
        print(f"Ошибка при обновлении: {e}")


# Запускать каждый час в :02 минуты
schedule.every().hour.at(":02").do(job)

if __name__ == '__main__':
    print("Планировщик запущен. Ожидание времени :02 каждой минуты часа...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверять каждую минуту