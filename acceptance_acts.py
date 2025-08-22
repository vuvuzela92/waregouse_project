from utils_warehouse import get_all_fbs_acts, create_insert_table_db
import pandas as pd

if __name__ == "__main__":
    

    # Получаем все акты-приема передачи и ФБО и ФБС
    df = get_all_fbs_acts()

    # Из полученных данных формируем акты-приема передачи для ФБО
    fbo_acts_df = df[['№ п\п', 'Товар (наименование)',  'Ед. изм.', 'Фактически принято - баркод', ' - артикул продавца', ' - сорт, размер', ' - КИЗ', ' - ШК короба', ' - кол-во', 'Документ','Номер_документа', 'Дата', 'account']]
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
        'account': 'account'
    })
    # Заменяем пустоты
    fbo_acts_df['kiz'] = fbo_acts_df['kiz'].fillna('Нет КИЗов')
    # Приводим колонку с датой к нужному формату
    fbo_acts_df['date'] = fbo_acts_df['date'].str.replace('"','').str.replace(' ', '').str.replace('г.', '')
    fbo_acts_df['date'] = pd.to_datetime(fbo_acts_df['date'], format='%d%m%Y')
    # Удаляем лишние символы из номера документа
    fbo_acts_df['document_number'] = fbo_acts_df['document'].str.extract(r'(\d+)\.zip')[0]
    print('Данные по ФБО полчены')

    # Из полученных данных формируем акты-приема передачи для ФБС
    fbs_acts_df = df[['№ п\п', 'Номер заказа', 'Ед. изм.', 'Фактически принято - Стикер/этикетка', ' - Кол-во', 'Документ','Номер_документа', 'Дата', 'account']]
    # ВБ иногда путает акты ФБО и ФБС, поэтому фильтруем по ШК короба
    fbs_acts_df = fbs_acts_df[fbs_acts_df['Фактически принято - Стикер/этикетка'].notna()]
    # Приводим названия колонок к читаемому виду
    fbs_acts_df = fbs_acts_df.rename(columns={
        '№ п\\п': 'num',
        'Номер заказа': 'order_number',
        'Ед. изм.': 'unit',
        'Фактически принято - Стикер/этикетка': 'sticker',
        ' - Кол-во': 'quantity',
        'Документ': 'document',
        'Номер_документа': 'document_number',
        'Дата': 'date',
        'account': 'account'
    })

    print('Данные по ФБС полчены')