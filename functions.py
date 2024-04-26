import time
import pandas as pd
import numpy as np

from geopy.geocoders import Nominatim
import cianparser

def parce_suburban(city, suburban_types = ["house",'townhouse'], pages_ranges = [[1,1],[1,1]],
                   parsed_pages=0,return_counter=False, old_df=pd.read_csv('suburban.csv')):
    
    """
    Парсит данные о загородной недвижимости с сайта Циан для заданного города и типов недвижимости.

    Args:
        city (str): Название города для парсинга.
        suburban_types (list): Список типов загородной недвижимости (например, "house", "townhouse").
        pages_ranges (list): Список диапазонов страниц для парсинга для каждого типа недвижимости.
        parsed_pages (int): Количество уже спарсенных страниц (для управления таймаутами).
        return_counter (bool): Если True, возвращает также количество спарсенных страниц.
        old_df (pd.DataFrame): DataFrame с ранее спарсенными данными для проверки на дубликаты.

    Returns:
        pd.DataFrame or tuple: DataFrame с новыми данными о загородной недвижимости или кортеж 
                               (DataFrame, количество спарсенных страниц), если return_counter=True.
    """
    
    parser = cianparser.CianParser(location=city)
    df = pd.DataFrame()
    data = []

    for suburban_type,pages_range in zip(suburban_types,pages_ranges):
        for page in range(pages_range[0], pages_range[1]+1):
            
            print(f'-----------------------------')
            print(f'Локация: {city}')
            print(f'Вид недвижимости: {suburban_type}')
            print()
            
            # Проверка есть-ли новые ссылки
            fast_suburban_sale = pd.DataFrame(parser.get_suburban( 
                                                deal_type="sale",
                                                suburban_type = suburban_type,
                                                additional_settings={"start_page":page,
                                                                    "end_page":page}
                                                ))
            if fast_suburban_sale.shape[0] > 0:
            
                ids = [x not in old_df['url'].to_list() for x in fast_suburban_sale['url'].to_list()]
                new_df = fast_suburban_sale[ids]
                new_size = new_df.shape[0]

                # Если есть, то скачивается страница с новыми данными
                if new_size > 0:
                    print(f'-----------------------------')
                    print(f'Найдено новых обьявлений: {new_size}')
                    print(f'-----------------------------')
                    if parsed_pages >= 2:
                        num = np.random.randint(100,120)
                        print(f'parsed pages: {parsed_pages}')
                        print(f'timeout: {num}')
                        time.sleep(num)
                        
                    suburban_sale = pd.DataFrame(parser.get_suburban( 
                                                        deal_type="sale",
                                                        suburban_type = suburban_type,
                                                        with_extra_data=True,
                                                        additional_settings={"start_page":page,
                                                                            "end_page":page}
                                                        ))
                    ids = [x not in old_df['url'].to_list() for x in suburban_sale['url'].to_list()]
                    good_sales = suburban_sale[ids]
                    
                    data.append(good_sales)
                    parsed_pages +=1
                else:
                    print(f'-----------------------------')
                    print(f'Новых обьявлений не найдено')
                    print(f'-----------------------------')
                    pass
            else:
                print(f'-----------------------------')
                print(f'Новых обьявлений не найдено')
                print(f'-----------------------------')
                pass

    # Список словарей из полученных данных преобразется в датафрейм

    if len(data) > 0:
        df = pd.concat(data).drop_duplicates().reset_index(drop=True)
    
    if return_counter:
        return df,parsed_pages
    else:
        return df
    
def create_address(row, reverse_order=False, drop_district=False, with_mo = False):
    """
    Формирует полный адрес объекта недвижимости из отдельных столбцов DataFrame.

    Args:
        row (pd.Series): Строка DataFrame, содержащая информацию об адресе.
        reverse_order (bool): Если True, формирует адрес в обратном порядке.

    Returns:
        str: Полный адрес объекта недвижимости.
    """
    country = 'Россия, '

    if with_mo:
        MO = 'Московская область, '
    else:
        MO = ''

    if reverse_order:
        if drop_district:
            address = country + MO + f"{row['location']}, {row['street']}, {row['house_number']}" 
        else:
            address = country + MO + f"{row['location']}, {row['district']}, {row['street']}, {row['house_number']}" 
    else:
        if drop_district:
            address = country + MO + f"{row['street']}, {row['house_number']}, {row['location']}" 
        else:
            address = country + MO + f"{row['street']}, {row['house_number']}, {row['district']}, {row['location']}"

    address = address.strip().replace(", , ,", ",").replace(", ,", ",")
    if address.startswith(","):
        address = address[1:]
    return address

geolocator = Nominatim(user_agent="my_app", timeout=60)

def geocode_address(row, all_country = True):
    """
    Геокодирует адрес с помощью OpenStreetMap API, пробуя различные варианты адреса.

    Args:
        row (pd.Series): Строка DataFrame, содержащая информацию об адресе.

    Returns:
        tuple: Координаты (latitude, longitude) объекта, если геокодирование успешно, иначе None.
    """



    address_options = [create_address(row.fillna(''), with_mo = True),
                        create_address(row.fillna(''), reverse_order=True, with_mo = True),
                        create_address(row.fillna(''), drop_district=True, with_mo = True),
                        create_address(row.fillna(''), reverse_order=True, drop_district=True, with_mo = True)]
    
    if all_country:
        address_options += [
        create_address(row.fillna(''), with_mo=False),
        create_address(row.fillna(''), reverse_order=True, with_mo=False),
        create_address(row.fillna(''), drop_district=True, with_mo=False),
        create_address(row.fillna(''), reverse_order=True, drop_district=True, with_mo=False),
    ]

    for address in address_options:
        try:
            location = geolocator.geocode(address)
            return (location.latitude, location.longitude)
        except:
            try:
                location = geolocator.geocode(address).replace('-я','')
                return (location.latitude, location.longitude)
            except:
                pass  # Продолжаем пробовать другие варианты адреса

    return None  # Если ни один вариант не сработал
  