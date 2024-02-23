"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
import psycopg2
import csv
from config import ORDERS_DIR, CUSTOMERS_DIR, EMPLOYEES_DIR

# Подключаемся к базе данных
conn = psycopg2.connect(host='localhost', database='north', user='postgres', password=os.getenv('PostgreSQL_PSW'))

# Заполняем таблицы данными из north_data
try:
    with conn:
        with conn.cursor() as cur:
            with open(EMPLOYEES_DIR, encoding="windows-1251") as csvfile:
                reader = csv.reader(csvfile)
                next(reader) # пропускаем заголовки
                # цикл для вставки данных в таблицу
                for row in reader:
                    query = 'INSERT INTO employees VALUES(%s,%s,%s,%s,%s,%s)'
                    cur.execute(query, row)

            with open(CUSTOMERS_DIR, encoding="windows-1251") as csvfile:
                reader = csv.reader(csvfile)
                next(reader) # пропускаем заголовки
                # цикл для вставки данных в таблицу
                for row in reader:
                    query = 'INSERT INTO customers VALUES(%s,%s,%s)'
                    cur.execute(query, row)

            with open(ORDERS_DIR, encoding="windows-1251") as csvfile:
                reader = csv.reader(csvfile)
                next(reader) # пропускаем заголовки
                # цикл для вставки данных в таблицу
                for row in reader:
                    query = 'INSERT INTO orders VALUES(%s,%s,%s,%s,%s)'
                    cur.execute(query, row)

finally:
    conn.close()