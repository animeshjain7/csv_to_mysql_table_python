import mysql.connector
import pandas as pd
import time
host = "localhost"
user = 'root'
password = '#YOUR PASSWORD'                               #put mysql password
database = '#YOUR DATABASE'                                # put mysql databse name
port = 3306

cursor = None
connection = None
try:
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    if connection.is_connected():
        print('Connectd to mysql')
        cursor = connection.cursor()
        file = pd.read_csv('Housing.csv')                   # read csv

      
        l = []
        my_dict = {}
        for a in file.values[1]:
            l.append(type(a))
            my_dict = dict(zip(file.columns, l))
          
        create_table_query = ""
        sql_dtype = ''
      
        for keys, values in my_dict.items():
            if my_dict[keys] == int:
                sql_dtype = 'INT'
            if my_dict[keys] == str:
                sql_dtype = 'VARCHAR (255)'
            create_table_query += f"{keys} {sql_dtype},\n"

        create_table_query = "CREATE TABLE #table_name(" + create_table_query                # put your table name here at #table_name
        create_table_query = create_table_query[:-2] + ");"
        print(create_table_query)
        cursor.execute(create_table_query)
        connection.commit()
        print("Creating Table in MySql")


      
        count = 0
        for item in file.values:
            count += 1
            insert_query = f'INSERT INTO #table_name VALUES('                            #  put your table name here at #table_name
            for a in item:
                if type(a) == int:
                    insert_query += f"{a},"
                else:
                    insert_query += f"'{a}',"
            insert_query = insert_query[:-1] + ');'
            print(f'inserting {count} data')
            cursor.execute(insert_query)
            connection.commit()
            print(insert_query)

except mysql.connector.Error as e:
    print('MYsql error:', e)
finally:
    if cursor is not None:
        cursor.close()
    if connection is not None and connection.is_connected():
        connection.close()
        print('Connection Closed')
