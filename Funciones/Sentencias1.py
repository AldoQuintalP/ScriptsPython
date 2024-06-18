import logging
import pandas as pd
import mysql.connector
import configparser
from Funciones import Descompress as s

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create(exportar, report, branch, cnn):
    with cnn.cursor() as cursor:
        # Lee el archivo CSV en un DataFrame de Pandas
        df = pd.read_csv(exportar)

        # Crea una tabla en MySQL con las columnas y tipos de datos especificados en el archivo CSV
        create_table_query = f"CREATE TABLE IF NOT EXISTS {report}{branch} ("

        for _, row in df.iterrows():
            columna = row["col"]
            tipo_dato = row["dato"]
            create_table_query += f"{columna} {tipo_dato}, "
                
        create_table_query = create_table_query.rstrip(', ')  # Elimina la última coma y espacio
        create_table_query += ") DEFAULT CHARACTER SET latin1;"

        try:
            cursor.execute(create_table_query)
            cnn.commit()
            logger.info("Tabla creada correctamente en MySQL.")
        except mysql.connector.Error as err:
            logger.error(f"Error de MySQL: {err}")

def drop(report, branch, client, cnn):
    with cnn.cursor() as cursor:
        logger.info(f"********** Limpiando base {report} del cliente: {client} **********")
        sql = f"DROP TABLE {report}{branch}"
        try:
            cursor.execute(sql)
            cnn.commit()
            logger.info("********** Base limpiada correctamente **********")
        except mysql.connector.Error as err:
            logger.error(f"********** Error al limpiar base: {err} **********")
            cnn.rollback()

def insert(report, branch, cnn, values, values2, tuplas):
    logger.info(f"********** Agregando registros a la base {report} **********")
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"
    try:
        with cnn.cursor() as cursor:
            cursor.executemany(sql, tuplas)
            cnn.commit()
            logger.info("********** Registros agregados correctamente **********")
    except mysql.connector.Error as err:
        cnn.rollback()
        logger.error(f"********** Error al cargar los registros: {err} **********")

def export(report, branch, cnn):
    table_name = f"{report}{branch}"
    dump = (f"-- MySQL dump\n--\n"
            f"-- Host: localhost    Database: sim_\n"
            f"-- ------------------------------------------------------\n"
            f"-- Server version {cnn.get_server_info()}\n")

    try:
        with cnn.cursor() as cursor:
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

            value_strings = [f"({', '.join([f'\'{str(value)}\'' if value is not None else 'NULL' for value in row])})" for row in data]
            dump += ', '.join(value_strings) + ';\n'

        export_path = s.ruta['Sandbx'] + report + branch + '.sql.dump'
        logger.info(f'Export path: {export_path}')
        with open(export_path, 'w') as f:
            f.write(dump)

        logger.info(f"Exportación completada en {report}{branch}.sql")
    except mysql.connector.Error as err:
        logger.error(f"Error durante la exportación: {err}")

def change(report, branch, cnn, exportar):
    df = pd.read_csv(exportar)
    columnas_a_cambiar = ["Costo", "CostoCompra", "CostoActual", "CostoUnit"]

    try:
        with cnn.cursor() as cursor:
            for _, row in df.iterrows():
                nombre_actual = row["col"]
                if nombre_actual in columnas_a_cambiar:
                    nuevo_nombre = f"{nombre_actual}$"
                    tipo_dato = row["dato"]
                    alter_query = f"ALTER TABLE {report}{branch} CHANGE {nombre_actual} {nuevo_nombre} {tipo_dato};"
                    cursor.execute(alter_query)
        logger.info("Cambio de columnas completado correctamente.")
    except mysql.connector.Error as err:
        logger.error(f"Error durante el cambio de columnas: {err}")
