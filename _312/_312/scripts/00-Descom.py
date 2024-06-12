import os
import subprocess
import glob
import chardet
import codecs
from Funciones import Descompress as d

# Call the dir function in Descompress to navigate to the 2-Working path
directorio = d.dir()
archivos = [nombre_archivo for nombre_archivo in os.listdir(directorio) if os.path.isfile(os.path.join(directorio, nombre_archivo))]

for nombre_archivo in archivos:
    paquete = nombre_archivo
    if len(paquete) >= 8:
        client1 = paquete[:4]
        client = client1.lstrip('0')
        branch = paquete[4:6]
        d.bands(client, branch)
        d.descomprimir(paquete)

layouts = ["CARTER", "DATPER", "FACEN", "FACES", "FACNOE", "INVNUE", "INVUSA", "MACC", "MACP", "PROVED", "REFINV", "REFCOM", "REFOEP", "REFSER", "REFVTA", "REFMOS", "SEROEP", "SERTEC", "SERVTA", "VTANUE", "VTAUSA", "CRMNUE", "NUEADC"]
os.chdir(d.sandbox)

for reportes in layouts:
    matching_files = glob.glob(f"*{reportes}*.txt")
    for filepath in matching_files:
        if reportes == "SERVTA":
            with open(filepath, 'rb') as archivo:
                resultado = chardet.detect(archivo.read())
                codificacion = resultado['encoding']
            with codecs.open(filepath, 'r', encoding=codificacion) as archivo:
                contenido = archivo.read()
            base_filename = os.path.basename(filepath)
            new_filename = base_filename.replace(filepath, f"{reportes}{branch}.txt")
            new_filepath = os.path.join(os.path.dirname(filepath), new_filename)
            os.rename(filepath, new_filepath)
            with codecs.open(new_filepath, 'w', encoding='utf-8') as archivo_utf8:
                archivo_utf8.write(contenido)
        else:
            base_filename = os.path.basename(filepath)
            new_filename = base_filename.replace(filepath, f"{reportes}{branch}.txt")
            new_filepath = os.path.join(os.path.dirname(filepath), new_filename)
            os.rename(filepath, new_filepath)

rutar = d.dir2()
lista = os.listdir(rutar)

for archivo in lista:
    if f"REFVTA{branch}" in archivo:
        ruta_completa = os.path.join(rutar, archivo)
        if ruta_completa:
            nuevo_nombre = os.path.join(rutar, archivo.replace("REFVTA", "REFMOS"))
            os.rename(ruta_completa, nuevo_nombre)

os.chdir("C:\\Users\\Aldo Quintal\\Clients")