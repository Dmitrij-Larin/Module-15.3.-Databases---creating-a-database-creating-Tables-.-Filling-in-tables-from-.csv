import os

import pandas as pd  # Нашёл в гугле и решил попробовать данную библиотеку
import pyodbc
from dotenv import load_dotenv

load_dotenv()


class DBManager:
    """Класс для создания и редактирования БД"""

    def __init__(self, driver, server, database, username, password):
        """Инициализация параметров для подключения к SSMS"""
        self.connection_string = fr"""DRIVER={driver};
                                      SERVER={server};
                                      DATABASE={database};
                                      UID={username};
                                      PWD={password}"""
        self.connection = pyodbc.connect(self.connection_string)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def create_db(self, db_name):
        """Создание новой БД"""
        try:
            self.cursor.execute(fr"CREATE DATABASE {db_name}")
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            self.connection.commit()
            print(f"База данных {db_name} успешно создана.")

    def create_tables(self, db_name):
        """Создание таблиц в только что созданной БД"""
        try:
            self.cursor.execute(fr"USE {db_name}")

            self.cursor.execute("""CREATE TABLE Customers_data
                                (customer_id NVARCHAR(10) PRIMARY KEY,
                                company_name NVARCHAR(100),
                                contact_name NVARCHAR(50))""")

            self.cursor.execute("""CREATE TABLE Employees_data
                                (employee_id INT PRIMARY KEY IDENTITY(1,1),
                                first_name NVARCHAR(100),
                                last_name NVARCHAR(100),
                                title NVARCHAR(100),
                                birth_date DATE,
                                notes NVARCHAR(1000))""")

            self.cursor.execute("""CREATE TABLE Orders_data
                                (order_id INT PRIMARY KEY,
                                customer_id NVARCHAR(10),
                                employee_id INT,
                                order_date DATE,
                                ship_city NVARCHAR(100),
                                FOREIGN KEY (customer_id) REFERENCES Customers_data (customer_id),
                                FOREIGN KEY (employee_id) REFERENCES Employees_data (employee_id))""")
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            self.connection.commit()
            print("Таблицы в БД успешно созданы.")

    def load_csv_to_table(self, db_name):
        """Заполнение таблиц данными из CSV файлов"""
        self.cursor.execute(fr"USE {db_name}")

        df_customers = pd.read_csv('customers_data.csv')
        for index, row in df_customers.iterrows():
            try:
                customer_id = row['customer_id']
                company_name = row['company_name'].replace("'", "''")
                # Пришлось преобразовывать, так как есть данные, которые не давали корректно заполнить таблицу
                contact_name = row['contact_name']
                self.cursor.execute(f"""
                INSERT INTO Customers_data (customer_id, company_name, contact_name)
                VALUES (?, ?, ?)""", customer_id, company_name, contact_name)
            except Exception as e:
                print(f'Ошибка при вставке строки {index}: {e}')
            print(f"Данные из customers_data.csv успешно заргужены в {db_name}.")

        df_employees = pd.read_csv('employees_data.csv')
        df_employees['employee_id'] = range(1, len(df_employees) + 1)  # Добавляем уникальные идентификаторы
        for index, row in df_employees.iterrows():
            try:
                first_name = row['first_name']
                last_name = row['last_name']
                title = row['title']
                birth_date = row['birth_date']
                notes = row['notes']
                self.cursor.execute(f"""
                INSERT INTO Employees_data (first_name, last_name, title, birth_date, notes)
                VALUES (?, ?, ?, ?, ?)""", first_name, last_name, title, birth_date, notes)
            except Exception as e:
                print(f'Ошибка при вставке строки {index}: {e}')
            print(f"Данные из employees_data.csv успешно заргужены в {db_name}.")

        df_orders = pd.read_csv('orders_data.csv')
        for index, row in df_orders.iterrows():
            try:
                order_id = row['order_id']
                customer_id = row['customer_id']
                employee_id = row['employee_id']
                order_date = row['order_date']
                ship_city = row['ship_city']
                self.cursor.execute(f"""
                INSERT INTO Orders_data (order_id, customer_id, employee_id, order_date, ship_city)
                VALUES (?, ?, ?, ?, ?)""", order_id, customer_id, employee_id, order_date, ship_city)
            except Exception as e:
                print(f'Ошибка при вставке строки {index}: {e}')
            print(f"Данные из orders_data.csv успешно заргужены в {db_name}.")
        self.connection.commit()

    def close(self):
        """Закрытие подключение"""
        self.cursor.close()
        self.connection.close()

    """На случай, если нужно снести БД."""
    # def drop_db(self, db_name):
    #     self.cursor.execute(fr'ALTER DATABASE {db_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
    #     self.cursor.execute(fr'DROP DATABASE {db_name}')
    #     self.connection.commit()
    #     print(f"База данных {db_name} успешно снесена.")


if __name__ == '__main__':
    DRIVER = os.getenv('MS_SQL_DRIVER')
    SERVER = os.getenv('MS_SQL_SERVER')
    DATABASE = os.getenv('MS_SQL_DATABASE')
    PAD_DATABASE = os.getenv('MS_PAD_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')

    db_manager = DBManager(DRIVER, SERVER, PAD_DATABASE, USER, PASSWORD)

    db_manager.create_db('NorthWind')
    db_manager.close()

    db_manager.create_tables('NorthWind')
    db_manager.close()

    db_manager.load_csv_to_table('NorthWind')
    db_manager.close()

    # db_manager.drop_db('NorthWind')
    # db_manager.close()
