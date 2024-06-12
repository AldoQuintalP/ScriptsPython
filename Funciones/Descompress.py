from zipfile import ZipFile
import os , subprocess
from datetime import datetime
import os
import yaml 


with open("config.yml", 'r') as ymlfile:     
     cfg = yaml.load(ymlfile, Loader=yaml.FullLoader) 
for section in cfg:     
     print(section) 

sandbox = cfg['paths']['sandbx'] 
working = cfg['paths']['workng'] 
client = cfg['paths']['clients']
path = cfg['paths']['path'] 
pathimp = cfg['paths']['pathImp'] 


def descomprimir(paquete):
    archivozip = working + paquete
    print(f'Archivo zip: {archivozip}')

    with ZipFile(file=archivozip,mode="r",allowZip64=True) as file:
        archivo=file.open(name=file.namelist()[0],mode="r")
        #print(archivo.read())
        archivo.close

        navegacion = sandbox
        print("Descomprimiendo Archivos...")
        file.extractall(path=navegacion)
        print("Archivo Descomprimido")


def bands(client,branch):

    navegacion = sandbox        
    textc=client
    directorio = navegacion
    band1 = directorio + "client.txt"

    # Abrir el archivo en modo escritura y guardar el contenido
    with open(band1, 'w') as archivo:
     archivo.write(textc)

    textb=branch
    directorio = navegacion
    band2 = directorio + "branch.txt"

    # Abrir el archivo en modo escritura y guardar el contenido
    with open(band2, 'w') as archivo:
     archivo.write(textb)

    fecha_actual = datetime.now().date()
    # Imprimir la fecha actual
    print(fecha_actual)

def dir():
   directorio=working

   return directorio

def dir2():
   directorio= sandbox

   return directorio
def ejecutar():
    ruta_archivo="C:\\Users\\Aldo Quintal\\Clients\\310\\scripts\\20.py"
    return ruta_archivo

def sandbx():
   print()