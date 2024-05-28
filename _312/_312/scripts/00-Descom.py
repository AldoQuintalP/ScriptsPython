import os , subprocess, glob, chardet, codecs
from Funciones import Descompress as d


#llama a la funcion dir en Descompress para poder ubicarnos en la ruta 2-Workng
directorio = d.dir()
#print(f'Directorio: {directorio}')
#Guarda lor archivos existentes en la ruta en una variable lista
archivos = [nombre_archivo for nombre_archivo in os.listdir(directorio) if os.path.isfile(os.path.join(directorio, nombre_archivo))]

# Puedo yo procesar mas de un archivo ?

#print(f'Archivos: {archivos}')
#Recorre la variable para poder almacenar los archivos a descomprimir
for nombre_archivo in archivos:
    #print(nombre_archivo)
    paquete=nombre_archivo #guarda el nombre del archivo en la variable paquete    
    #print(f'Paquete: {paquete}')
#si el archivo en >= 8 genera las banderas
if len(nombre_archivo) >= 8:  # Asegurarse de que el título sea lo suficientemente largo
        client1=nombre_archivo[:4]
        #print(f'Cliente1: {client1}')
        client=client1.lstrip('0') #si el numero de cliente empieza con 0 lo limpia
        #print(f'Client: {client}')
        branch = nombre_archivo[4:6]
        #print(f'Branch: {branch}')
        #print(client)
        #print(branch)

d.bands(client,branch) #se llama a la funcion bands para crear las banderas
d.descomprimir(paquete) #Se llama a la funcion para descomprimir el archivo

#puebas
#ruta1=os.getcwd()
#print(ruta1)

layouts = ["CARTER", "DATPER", "FACEN", "FACES", "FACNOE", "INVNUE", "INVUSA", "MACC", "MACP", "PROVED", "REFINV", "REFCOM", "REFOEP", "REFSER", "REFVTA", "REFMOS", "SEROEP", "SERTEC", "SERVTA", "VTANUE", "VTAUSA", "CRMNUE", "NUEADC"]
#print(f'Layouts: {layouts}')
os.chdir(d.sandbox)

for reportes in layouts:
    if reportes=="SERVTA":

        #print(reportes)
        matching_files = glob.glob(f"*{reportes}*.txt")
        #print(f'Matchig files: {matching_files}')
        #print(f"Matching files for {reportes}: {matching_files}")

        for filepath in matching_files:
            # Detectar la codificación del archivo
            with open(filepath, 'rb') as archivo:
                resultado = chardet.detect(archivo.read())
                #print(f'Resultado: {resultado}')
                codificacion = resultado['encoding']
                #print(f'Codificación: {codificacion}')

            # Abrir el archivo en modo lectura con la codificación original
            with codecs.open(filepath, 'r', encoding=codificacion) as archivo:
                contenido = archivo.read()
                

            # Crear el nuevo nombre de archivo UTF-8
            base_filename = os.path.basename(filepath)
            #print(f'Base_filename: {base_filename}')
            new_filename = base_filename.replace(filepath, f"{reportes}{branch}.txt")
            #print(f'New_filename: {new_filename}')
            new_filepath = os.path.join(os.path.dirname(filepath), new_filename)
            #print(f'New_filepath: {new_filepath}')
            os.rename(filepath, new_filepath)

            # Guardar el contenido en el nuevo archivo con la codificación UTF-8
            with codecs.open(new_filepath, 'w', encoding='utf-8') as archivo_utf8:
                archivo_utf8.write(contenido)
    else:
         matching_files = glob.glob(f"*{reportes}*.txt")
    #print(f"Matching files for {reportes}: {matching_files}")

    for filepath in matching_files:
        #print(filepath)
        base_filename = os.path.basename(filepath)
        new_filename = base_filename.replace(filepath, f"{reportes}{branch}.txt")
        new_filepath = os.path.join(os.path.dirname(filepath), new_filename)
        os.rename(filepath, new_filepath)

rutar=d.dir2()
#print(f'Rutar: {rutar}')
lista = os.listdir(rutar)
#print(f'Lista: {lista}')
#print(lista)
    # Itera a través de la lista de archivos
for archivo in lista:

    # Verifica si el archivo actual se llama "REFVTA"
    #print(f'Archivo: {archivo}')
    if f"REFVTA{branch}" in archivo:
        ruta_completa = os.path.join(rutar, archivo)
        #print(f'ruta_completa: {ruta_completa}')
        if not ruta_completa:
            nuevo_nombre = os.path.join(rutar, archivo.replace("REFVTA", "REFMOS"))
            os.rename(ruta_completa, nuevo_nombre)



os.chdir("C:\\Users\\simetrical\\Clients")

#ruta=os.getcwd() 
#print(ruta)       
#proceso=d.ejecutar()
#subprocess.run(["python", proceso], shell=True)