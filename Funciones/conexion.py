import sys
import pymysql
import json 
import mysql.connector
import configparser

# Toma la info del archivo de config
# config = configparser.ConfigParser()
# config.read('config.cfg')
# conn_sql = config['MySqlbd']

# print("Probando conexión ...")

# connection = mysql.connector.connect(
#             host= conn_sql['host'],
#             user=conn_sql['user'],
#             password=conn_sql['password'],
#             auth_plugin="mysql_native_password"
#         )

# print(f'Conexión: {connection}')

def cbase(client):

    connection = None
    cursor = None
    

    try:
        # Conéctate al servidor MySQL
        connection = mysql.connector.connect(
            host= "127.0.0.1",
            user="root",
            password="1234",
            #auth_plugin="mysql_native_password"
        )
        
        # Verifica que la conexión se haya establecido correctamente
        if connection.is_connected():
            cursor = connection.cursor()

            check_database_query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'sim_{client}'"
            #print(check_database_query)
            cursor.execute(check_database_query)
            result = cursor.fetchone()

            if not result:

            # Comando SQL para crear la base de datos
                create_database_query = f"CREATE DATABASE sim_{client}"

            # Ejecuta el comando SQL para crear la base de datos
                cursor.execute(create_database_query)
                print(f"Base de datos sim_'{client}' creada con éxito.")
    except mysql.connector.Error as err:
        print(f"Error al crear la base de datos: {err}")
    finally:
        # Cierra el cursor y la conexión si están abiertos
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def conect(client):
    try:
        rds_host  = "127.0.0.1"
        user_name ="root"
        password = "1234"
        db_name = f"sim_{client}"
        port = 3306
        auth_plugin="mysql_native_password"
        #table_name = report

        print(f'host: {rds_host}, User: {user_name}, pass: {password}, db_name: {db_name} .......................')


        print("Estableciendo Conexion")
        conn = pymysql.connect(host=rds_host, user=user_name, passwd=password, db=db_name, port=port, connect_timeout=60)
        print("Conexion establecida")
    except(pymysql.err.OperationalError, pymysql.err.InternalError) as e:
         print("Ocurrio un error al conectarse: ",e)

    
    return conn


def dbase(client):

    connection = None
    cursor = None

    try:
        # Conéctate al servidor MySQL
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="1234"
        )

        # Verifica que la conexión se haya establecido correctamente
        if connection.is_connected():
            cursor = connection.cursor()

            check_database_query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'sim_{client}'"
            #print(check_database_query)
            cursor.execute(check_database_query)
            result = cursor.fetchone()

            if result:
                # Si la base de datos ya existe, elimínala
                drop_database_query = f"DROP DATABASE sim_{client}"
                cursor.execute(drop_database_query)
                print(f"Base de datos 'sim_{client}' eliminada con éxito.")
            
    except mysql.connector.Error as err:
        print(f"Error al crear la base de datos: {err}")
    finally:
        # Cierra el cursor y la conexión si están abiertos
        if cursor:
            cursor.close()
        if connection:
            connection.close()
   