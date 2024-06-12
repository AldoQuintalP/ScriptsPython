import pandas as pd
import mysql.connector
import configparser
from Funciones import Descompress as s

def create(exportar,report,brach,cnn):
    cursor= cnn.cursor()
            # Lee el archivo CSV en un DataFrame de Pandas
    df = pd.read_csv(exportar)

            # Crea una tabla en MySQL con las columnas y tipos de datos especificados en el archivo CSV
    create_table_query = f"CREATE TABLE IF NOT EXISTS {report}{brach} ("

    for index, row in df.iterrows():
                columna = row["col"]
                tipo_dato = row["dato"]
                create_table_query += f"{columna} {tipo_dato}, "
                
    create_table_query = create_table_query[:-2]  # Elimina la última coma y espacio
    create_table_query += f") DEFAULT CHARACTER SET latin1;"

    try:
            cursor.execute(create_table_query)
            cnn.commit()
            print("Tabla creada correctamente en MySQL.")
    except mysql.connector.Error as err:
                print(f"Error de MySQL: {err}")

def drop(report,branch,client,cnn):
        cursor = cnn.cursor()
        print("********** Limpiando base " + report + " Del cliente: " + client + " **********")
        sql = f"DROP TABLE {report}{branch}"
        try:
                cursor.execute(sql)
                cnn.commit()
                print("********** Base Limpiada Correctamente **********")
        except:
                print("********** Error al Limpiar Base **********")
                cnn.rollback()

def insert(report,branch,cnn,values,values2,tuplas):
        print("********** Agregando registros..." + report + " a la base: " + report + " **********")
        cursor = cnn.cursor()

        #(Client,Branch,Date,NumeroOT,FechaEntrega,FechaCierre,Taller,TipoOrden,TipoServicio,Motivo,Status,NumeroParte,Descripcion,Cantidad,VentaUnit,Venta,CostoUnit,Costo,Utilidad,Margen,Vin,RFC,Modelo,Version)
        sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

        try:
                cursor.executemany(sql,tuplas)
                cnn.commit()
                print("********** Registros agregados correctamente **********")
        
        except:
                cnn.rollback()
                print("********** Error al cargar los registros **********")
                cnn.close()

def export(report,branch,cnn):
    # Crea un cursor
    cursor = cnn.cursor()

    # Nombre de la tabla
    table_name = f'{report}{branch}'

    # Genera el encabezado del dump
    dump = f"-- MySQL dump\n" \
        f"--\n" \
        f"-- Host: localhost    Database: sim_\n" \
        f"-- ------------------------------------------------------\n" \
        f"-- Server version {cnn.get_server_info()}\n"

    # Obtiene la estructura de la tabla
    cursor.execute(f"SHOW CREATE TABLE {table_name}")
    table_structure = cursor.fetchone()
    table_create_statement = table_structure[1]

    # Genera el encabezado para la tabla
    dump += f"\n\n-- Table structure for table `{table_name}`\n\n"
    dump += f"DROP TABLE IF EXISTS `{table_name}`;\n"
    dump += table_create_statement + ";\n"

    # Genera el encabezado para los datos
    dump += f"\n-- Dumping data for table `{table_name}`\n\n"
    dump += f"INSERT INTO `{table_name}` VALUES "

    # Obtiene y exporta los datos de la tabla
    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()

    # Formatea los valores de datos para la inserción masiva
    value_strings = []
    for row in data:
        values = ", ".join([f"'{str(value)}'" if value is not None else "NULL" for value in row])
        value_strings.append(f"({values})")

    # Combina los valores en una sola línea
    dump += ', '.join(value_strings) + ';\n'

    # Cierra el cursor y la conexión
    cursor.close()
    cnn.close()
    
    export_path= s.ruta['Sandbx'] + report +''+ branch +'.sql.dump'
    print(f'Export path ... {export_path}')
    # Guarda el dump en un archivo
    with open(export_path, 'w') as f:
        f.write(dump)

    print("Exportación completada en "+report+""+branch+".sql")

def change(report,branch,cnn,exportar):
   # Lee el archivo CSV en un DataFrame de Pandas
    df = pd.read_csv(exportar)

 # Columnas a modificar
    columnas_a_cambiar = ["Costo", "CostoCompra","CostoActual","CostoUnit"]  # Agrega las columnas que deseas modificar

 # Itera a través de las filas del archivo CSV
    for index, row in df.iterrows():
        nombre_actual = row["col"]
        nuevo_nombre = f"{nombre_actual}$"  # Agrega el signo "$" al nombre
        tipo_dato = row["dato"]  # Obtiene el tipo de dato del archivo CSV

    # Verifica si el nombre de la columna está en la lista de columnas a cambiar
        if nombre_actual in columnas_a_cambiar:
        # Genera una consulta SQL para modificar la tabla
            alter_query = f"ALTER TABLE {report}{branch} CHANGE {nombre_actual} {nuevo_nombre} {tipo_dato};"

        # Ejecuta la consulta SQL
            cursor = cnn.cursor()
            cursor.execute(alter_query)
            cursor.close()