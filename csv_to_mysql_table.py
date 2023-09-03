import mysql.connector
import pandas as pd
import time


host = "localhost"
user = 'root'
password = 'root'                                                              
port = 3306

cursor = None
connection = None
try:
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port
    )
   

    if connection.is_connected():
        cursor = connection.cursor()
        print('\t+-------------------------------+')
        print('\t| TYPE 0 TO USE OLD DATABASE    |')
        print('\t| TYPE 1 TO CREATE NEW DATABASE |')
        print('\t+-------------------------------+')
        choice = int(input('\tCHOICE FOR DATABASE : '))

        if choice == 0:
            show_db_query = "SHOW DATABASES;"
            cursor.execute(show_db_query)
            databases = cursor.fetchall()
            for x in databases:
                print('\t' + x[0])
            db_name = input('\tTYPE THE DATABASE YOU WANT TO USE : ')
            use_db_query = "USE "+ db_name + "; "
            cursor.execute(use_db_query)
        if choice == 1:
            new_db_name = input('\tENTER DATABASE NAME : ')
            create_new_db_query = "CREATE DATABASE IF NOT EXISTS " + new_db_name + "; "
            use_db_query = "USE "+new_db_name + "; "
            cursor.execute(create_new_db_query)
            cursor.execute(use_db_query)

        connection.commit()
        print('\tConnected to MySQL')
        file_path = input('\tEnter the path of CSV file : ')        #CTRL+SHIFT+V for paste in powershell
        file = pd.read_csv(file_path)                  

      
        l = []
        my_dict = {}
        for a in file.values[1]:
            l.append(type(a))
            my_dict = dict(zip(file.columns, l))
          
        create_table_query = ""
        new_table_name = input('\tENTER NEW TABLE NAME : ')
        sql_dtype = ''
      
        for keys, values in my_dict.items():
            if my_dict[keys] == int:
                sql_dtype = 'INT'
            if my_dict[keys] == str:
                sql_dtype = 'VARCHAR (255)'
            create_table_query += f"{keys} {sql_dtype},\n\t"

        create_table_query = "CREATE TABLE "+ new_table_name +" (" + create_table_query                
        create_table_query = create_table_query[:-3] + ");"
        print('\t'+create_table_query)
        cursor.execute(create_table_query)
        connection.commit()
        print("\tCreating Table in MySql")


      
        count = 0
        for item in file.values:
            count += 1
            insert_query = f'INSERT INTO {new_table_name} VALUES('                            
            for a in item:
                if type(a) == int:
                    insert_query += f"{a},"
                else:
                    insert_query += f"'{a}',"
            insert_query = insert_query[:-1] + ');'
            print('\t'+f'inserting {count} data')
            cursor.execute(insert_query)
            connection.commit()
            print('\t'+insert_query)
            time.sleep(0.2)
             
except mysql.connector.Error as e:
    print('\tMYsql error:', e)
finally:
    if cursor is not None:
        cursor.close()
    if connection is not None and connection.is_connected():
        connection.close()
        print('\tConnection Closed')
