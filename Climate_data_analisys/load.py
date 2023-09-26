# Загрузка данных и поиск минимальной температуры
# программа будет запускаться из командной строки,
# а на вход она будет принимать список станций через запятую и интервал лет через дефис

import collections
import csv
import datetime
import sys

import requests

stations = sys.argv[1].split(",")
years = [int(year) for year in sys.argv[2].split("-")]
start_year = years[0]
end_year = years[1]

TEMPLATE_URL =  "https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{station}.csv"
TEMPLATE_FILE = "data_files/station_{station}_{year}.csv"

def download_data(station, year):
    '''
    записывает на диск файл для запрашиваемой станции и года
    :param station: станция
    :param year: год
    :return: None
    '''
    my_url = TEMPLATE_URL.format(station=station, year=year)
    req = requests.get(my_url)
    if req.status_code != 200:
        return "НЕ НАЙДЕН"
    w = open(TEMPLATE_FILE.format(station=station, year=year), "wt")
    w.write(req.text)
    w.close()

def download_all_data(stations, start_year, end_year):
    '''
    для каждой станции и года ИЗ всех запрашиваемых станций по всем годам из
    заданного интервала.
    :param stations: список станций
    :param start_year: начало интервала
    :param end_year: конец интервала
    :return: None
    '''
    for station in stations:
        for year in range(start_year, end_year + 1):
            download_data(station, year)


def get_file_temperatures(file_name):
    '''
    извлечь все значения температуры из одного файла
    :param file_name: файл
    :return: значение температуры по одному при каждом обращении
    '''
    with open(file_name, "rt") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            station = row[header.index("STATION")]
            # date = datetime.datetime.fromisoformat(row[header.index('DATE')])
            tmp = row[header.index("TMP")]
            temperature, status = tmp.split(",")
            if status != "1":
                continue    # игнорируем строки, для которых данные недоступны
            temperature = int(temperature) / 10
            yield temperature

def get_all_temperatures(stations, start_year, end_year):
    '''
    извлечекает все показания температуры
    :param stations: список станций
    :param start_year: начиная с года
    :param end_year: по конечный год
    :return: список температур
    '''
    temperatures = collections.defaultdict(list)
    for station in stations:
        for year in range(start_year, end_year + 1):
            for temperature in get_file_temperatures(TEMPLATE_FILE.format(station=station, year=year)):
                temperatures[station].append(temperature)
    return temperatures