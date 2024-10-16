import csv
import sqlite3

#delimitator 
def detect_delimiter(filename):
    with open(filename, mode='r', encoding='utf-8') as file:
        first_line = file.readline()
        if first_line.count(';') > first_line.count(','):
            return ';'
        else:
            return ','

#read and update data
def load_data(filename):
    mylist = []
    with open(filename, mode='r', encoding='utf-8') as file:
        file_data = csv.reader(file, delimiter=detect_delimiter(filename))
        for row in file_data:
            mylist.append(row)
    return mylist

#create table 
def create_table(cursor, table_name, column_names):
    columns_with_types = ', '.join([f"{name} TEXT" for name in column_names])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})")

#insert data into table
def insert_data(cursor, table_name, data):
    placeholders = ', '.join(['?'] * len(data[0]))  
    for row in data:
        if len(row) == len(data[0]):  
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
        else:
            print(f"Skipping row due to incorrect length: {row}")  # Debug 

#convert CSV in SQL
def convert_csv_to_db():
    conn = sqlite3.connect('final_db.db')  # Connect to db
    cursor = conn.cursor()
    filenames = {
        "facebook": (''), 
        "google": (''),     # path to csv files 
        "website": ('')
    }

    for key, (filename, table_name) in filenames.items():
        try:
            data = load_data(filename)  
            column_names = data[0]  
            create_table(cursor, table_name, column_names)  
            insert_data(cursor, table_name, data[1:])  
            print(f"{table_name} data loaded successfully!")
        except Exception as e:
            print(f"An error occurred while loading {table_name} data: {e}")

    conn.commit()  
    conn.close()  

convert_csv_to_db()
