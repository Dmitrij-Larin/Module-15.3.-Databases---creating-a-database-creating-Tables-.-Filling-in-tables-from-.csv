import pandas as pd

def load_csv_to_table(self, db_name):
    try:
        self.cursor.execute(fr"USE {db_name}")

        df_customers = pd.read_csv('customers_data.csv')
        for index, row in df_customers.iterrows():
            insert_query = f"""
            INSERT INTO Customers_data (customer_id, company_name, contact_name)
            VALUES ('{row['customer_id']}', '{row['company_name']}', '{row['contact_name']}')"""
            self.cursor.execute(insert_query)
            print(f"Данные из customers_data.csv успешно заргужены в {db_name}.")

        df_employees = pd.read_csv('employees_data.csv')
        df_employees['employee_id'] = range(1, len(df_employees) + 1)  # Добавляем уникальные идентификаторы
        for index, row in df_employees.iterrows():
            insert_query = f"""
            INSERT INTO Employees_data (employee_id, first_name, last_name, title, birth_data, notes)
            VALUES ({row['employee_id']}, '{row['first_name']}', '{row['last_name']}', '{row['title']}',
            '{row['birth_data']}', '{row['notes']}')"""
            self.cursor.execute(insert_query)
            print(f"Данные из employees_data.csv успешно заргужены в {db_name}.")

        df_orders = pd.read_csv('orders_data.csv')
        for index, row in df_orders.iterrows():
            insert_query = f"""
            INSERT INTO Orders_data (order_id, customer_id, employee_id, order_date, ship_city)
            VALUES ({row['order_id']}, '{row['customer_id']}', {row['employee_id']}, '{row['order_date']}',
            '{row['ship_city']}')"""
            self.cursor.execute(insert_query)
            print(f"Данные из orders_data.csv успешно заргужены в {db_name}.")

    except pyodbc.ProgrammingError as ex:
        print(ex)
    else:
        self.connection.commit()