import pandas as pd
import mysql.connector
from Funciones import Descompress as s
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create(exportar, report, branch, cnn):
    cursor = cnn.cursor()
    df = pd.read_csv(exportar)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {report}{branch} ("

    for index, row in df.iterrows():
        columna = row["col"]
        tipo_dato = row["dato"]
        create_table_query += f"{columna} {tipo_dato}, "
                
    create_table_query = create_table_query[:-2]  # Eliminate the last comma and space
    create_table_query += f") DEFAULT CHARACTER SET latin1;"

    try:
        cursor.execute(create_table_query)
        cnn.commit()
        logging.info(f"Table {report}{branch} created successfully in MySQL.")
    except mysql.connector.Error as err:
        logging.info(f"MySQL Error: {err}")


def drop(report, branch, client, cnn):
    cursor = cnn.cursor()
    table_name = f"{report}{branch}"
        
    logging.info(f"Cleaning base {report} for client: {client}")
    sql = f"DROP TABLE IF EXISTS {table_name}"
    try:
        cursor.execute(sql)
        cnn.commit()
        logging.info("Base Cleaned Successfully")
    except mysql.connector.Error as err:
        logging.info("Base Cleaned")
        cnn.rollback()


def insert(report, branch, cnn, values, values2, tuplas):
    logging.info(f"Adding records to {report} in the database: {report}")
    cursor = cnn.cursor()

    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        logging.info("Records added successfully")
    except mysql.connector.Error as err:
        cnn.rollback()
        logging.info("Error loading records")
        cnn.close()


def export(report, branch, cnn, client):
    cursor = cnn.cursor()
    table_name = f'{report}{branch}'

    dump = f"-- MySQL dump\n" \
           f"--\n" \
           f"-- Host: localhost    Database: sim_{client}\n" \
           f"-- ------------------------------------------------------\n" \
           f"-- Server version {cnn.get_server_info()}\n"

    cursor.execute(f"SHOW CREATE TABLE {table_name}")
    table_structure = cursor.fetchone()
    table_create_statement = table_structure[1]

    dump += f"\n\n-- Table structure for table `{table_name}`\n\n"
    dump += f"DROP TABLE IF EXISTS `{table_name}`;\n"
    dump += table_create_statement + ";\n"

    dump += f"\n-- Dumping data for table `{table_name}`\n\n"
    dump += f"INSERT INTO `{table_name}` VALUES "

    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()

    value_strings = []
    for row in data:
        values = ", ".join([f"'{str(value)}'" if value is not None else "NULL" for value in row])
        value_strings.append(f"({values})")

    dump += ', '.join(value_strings) + ';\n'

    cursor.close()
    cnn.close()
    
    export_path = s.sandbox + report + '' + branch + '.sql.dump'
    logging.info(f'Exporting to {export_path}')
    with open(export_path, 'w') as f:
        f.write(dump)

    logging.info(f"Export completed for {report}{branch}.sql")


def change(report, branch, cnn, exportar):
    df = pd.read_csv(exportar)
    columnas_a_cambiar = ["CostoActual", "CostoUnit"]

    for index, row in df.iterrows():
        nombre_actual = row["col"]
        nuevo_nombre = f"{nombre_actual}$"
        tipo_dato = row["dato"]

        if nombre_actual in columnas_a_cambiar:
            alter_query = f"ALTER TABLE {report}{branch} CHANGE {nombre_actual} {nuevo_nombre} {tipo_dato};"
            cursor = cnn.cursor()
            cursor.execute(alter_query)
            cursor.close()
