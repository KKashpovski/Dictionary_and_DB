"""Приложение по учёту товаров на складе."""

import json
import jsonschema
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Any


def get_schema() -> dict:
    """Чтение JSON схемы."""
    with open("goods.schema.json", "r", -1, "UTF-8") as fd:
        my_schema = json.load(fd)
    return my_schema


def get_data() -> dict:
    """Чтение JSON данных."""
    with open("goods.data.json", "r", -1, "UTF-8") as fd:
        my_data = json.load(fd)
    return my_data


def validate_json() -> Any:
    """Валидация JSON данных и JSON схемы."""
    json_schema = get_schema()
    json_data = get_data()

    try:
        jsonschema.validate(json_data, json_schema)
        print("Данные JSON корректны и использованы в программе. \n")
        return True
    except jsonschema.exceptions.ValidationError:
        print("Данные JSON недопустимы. \n")
    except json.decoder.JSONDecodeError:
        print("Данные JSON недопустимы. \n")


def convert() -> dict:
    """Конвертация JSON данных в Python."""
    with open("goods.data.json", "r", -1, "UTF-8") as fd:
        data = json.loads(fd.read())
    return data


def create_db() -> bool:
    """Создание базы данных PostgreSQL."""
    try:
        # Подключение к PostgreSQL
        con = psycopg2.connect(
            user="postgres", password="123123", host="localhost", port="5432"
        )
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        sql_create_database = """create database les_sql_19"""
        cur.execute(sql_create_database)
        print("База данных успешно создана. \n")
        return True
    except (Exception, Error) as error:
        print("Ошибка при создании новой базы данных:", error, "\n")
        return False


def connect_db() -> Any:
    """Коннект с базой данных."""
    try:
        # Подключение к существующей базе данных PostgreSQL
        con = psycopg2.connect(
            database="les_sql_19",
            user="postgres",
            password="123123",
            host="localhost",
            port="5432",
        )

        # Курсор для выполнения операций с базой данных
        cur = con.cursor()
        # Выполнение SQL-запроса
        cur.execute("SELECT version();")
        # Получить результат
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error, "\n")
        return None
    return con


def create_table() -> None:
    """Создание таблиц в базе данных."""
    try:
        con = connect_db()
        cur = con.cursor()
        # Команда создает новую таблицу 1
        cur.execute(
            """CREATE TABLE IF NOT EXISTS goods
              (id SERIAL PRIMARY KEY,
              name VARCHAR NOT NULL,
              package_height FLOAT NOT NULL,
              package_width FLOAT NOT NULL)"""
        )
        # Команда создает новую таблицу 2
        cur.execute(
            """CREATE TABLE IF NOT EXISTS shops_goods
                      (id SERIAL PRIMARY KEY,
                      id_good INTEGER REFERENCES goods(id),
                      location VARCHAR NOT NULL,
                      amount INTEGER NOT NULL)"""
        )
        cur.execute("""CREATE UNIQUE INDEX idx ON shops_goods (id_good, location)""")
        # Команда фиксирует изменения
        con.commit()
        # con.close()
        print("Таблицы успешно созданы в базе данных. \n")
    except (Exception, Error) as error:
        print("Ошибка при создании таблиц:", error, "\n")


def insert_data_in_table() -> None:
    """Заполнение таблиц в базе данных."""
    con = connect_db()
    cur = con.cursor()
    data = convert()

    insert_1 = """INSERT INTO goods (id, name, package_height,
            package_width) VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
            name=EXCLUDED.name,
            package_height=EXCLUDED.package_height,
            package_width=EXCLUDED.package_width"""
    record_to_insert_1 = (
        data["id"],
        data["name"],
        data["package_params"]["height"],
        data["package_params"]["width"],
    )
    cur.execute(insert_1, record_to_insert_1)
    for elem in data["location_and_quantity"]:
        insert_2 = """INSERT INTO shops_goods (id_good, location, amount) VALUES (%s, %s, %s)
            ON CONFLICT (id_good, location) DO UPDATE SET
            amount=EXCLUDED.amount"""
        record_to_insert_2 = (data["id"], elem["location"], elem["amount"])
        cur.execute(insert_2, record_to_insert_2)
    con.commit()
    con.close()


def main() -> None:
    """Главная функция."""
    print("Приложение по учёту товаров на складе. \n")
    if validate_json() is True:
        if create_db() is True:
            create_table()
            insert_data_in_table()
            print("Записи в базе данных созданы!")
        else:
            create_table()
            insert_data_in_table()
            print("Записи в базе данных обновлены!")
    else:
        print("Вам нужно проверить корректность исходных данных.")
        exit()


main()
