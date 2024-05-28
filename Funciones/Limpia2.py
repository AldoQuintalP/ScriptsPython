import os
from Funciones import Descompress as s


def limpia2():
# Especifica el directorio que deseas limpiar
    directorio_a_limpiar = s.ruta['Sandbx'] 
    print(f'Directorio a limpiar : {directorio_a_limpiar}')

# Obtiene una lista de los nombres de archivo en el directorio
    archivos_en_directorio = os.listdir(directorio_a_limpiar)

# Itera sobre la lista de archivos y elimínalos uno por uno
    for archivo in archivos_en_directorio:
        ruta_completa = os.path.join(directorio_a_limpiar, archivo)
        if os.path.isfile(ruta_completa):
            os.remove(ruta_completa)
            print(f"Archivo eliminado: {ruta_completa}")

    print("Limpieza de archivos completada.")


# if __name__ == "__main__":
#     limpia2()