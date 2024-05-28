from unidecode import unidecode
import unicodedata
from pyspark.sql import functions as F

def LimpiaCodigos(texto):
    #Se Eliminan caracteres especiales antes y despues del texto
    temp = texto.upper()
    temp = temp.strip("""-+*/._:,;{}[]&lt;&gt;^`¨~´¡!¿?\'()=&amp;%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
    #Se reemplazan caracteres especiales por letras
    temp_rep = temp.upper().replace("Ã‘","N").replace("Ã‰","E").replace("Ã“","O").replace("Ãº","U").replace("Ã­","I").replace("Ã±","N")
    #Se eliminan acentos de letras Ej: Ã -> A
    final = unidecode(temp_rep)
    return final

def LimpiaCodigosv1(texto):
    # Convierte el texto a mayúsculas
    temp = texto.upper()
    # Elimina caracteres especiales antes y después del texto
    temp = temp.strip("""-+*/._:,;{}[]<>^`¨~´¡!¿?\'()=&%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
    # Reemplaza caracteres especiales por letras
    temp_rep = temp.replace("Ã‘","N").replace("Ã‰","E").replace("Ã“","O").replace("Ãº","U").replace("Ã­","I").replace("Ã±","N")
    # Elimina acentos de letras
    final = unidecode(temp_rep)
    return final

def LimpiaTexto(texto):
    # Eliminar caracteres especiales antes y después del texto
    if texto is not None:
       temp = texto.upper()
       temp = temp.strip("""-+*/._:,;{}[]&lt;&gt;^`¨~´¡!¿?\'()=&amp;%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
       # Reemplazar caracteres especiales por letras
       temp_rep = temp.upper().replace("Ã‘","N").replace("Ã‰","E").replace("Ã“","O").replace("Ãº","U")\
       .replace("Ã­","I").replace("Ã±","N").replace("‰","A").replace("¢","O").replace("Ã³","O")\
       .replace("/"," ").replace("-"," ").replace("_"," ").replace(".","").replace(",","")
       # Eliminar acentos de letras Ej: Ã -> A 
       final = unidecode(temp_rep)
    else:
       final = ""

    return final

def LimpiaTextov1(texto):
    if texto is not None:
        # Convertir a mayúsculas y quitar acentos
        temp = unidecode(texto.upper())
        # Eliminar caracteres especiales antes y después del texto
        temp = temp.strip("""-+*/._:,;{}[]<>&^`¨~´¡!¿?'()=&%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
        # Reemplazar caracteres especiales por letras
        temp_rep = (
            temp.replace("Ã‘","N")
                .replace("Ã‰","E")
                .replace("Ã“","O")
                .replace("Ãº","U")
                .replace("Ã­","I")
                .replace("Ã±","N")
                .replace("‰","A")
                .replace("¢","O")
                .replace("Ã³","O")
                .replace("/"," ")
                .replace("-"," ")
                .replace("_"," ")
                .replace(".","")
                .replace(",","")
        )
        # Eliminar acentos de letras
        final = unidecode(temp_rep)
    else:
        final = ""

    return final

def LimpiaTextov2(texto):
    if texto is not None:
        # Convertir a mayúsculas y normalizar Unicode
        temp = unicodedata.normalize('NFKD', texto.upper())
        # Eliminar caracteres especiales antes y después del texto
        temp = temp.strip("""-+*/._:,;{}[]<>&^`¨~´¡!¿?'()=&%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
        # Reemplazar caracteres especiales por letras
        temp_rep = (
            temp.replace("Ñ","N")
                .replace("É","E")
                .replace("Ó","O")
                .replace("Ú","U")
                .replace("Í","I")
                .replace("Ñ","N")
                .replace("‰","A")
                .replace("¢","O")
                .replace("Ó","O")  # Tratar específicamente el carácter 'ó'
                .replace("/"," ")
                .replace("-"," ")
                .replace("_"," ")
                .replace(".","")
                .replace(",","")
        )
        # Eliminar acentos de letras
        final = ''.join(char for char in temp_rep if unicodedata.category(char) != 'Mn')
    else:
        final = ""

    return final


def LimpiaEmail(texto):
    # Eliminar caracteres especiales antes y después del texto
    temp = texto.upper()
    temp = temp.strip("""-+*/._:,;{}[]&lt;&gt;^`¨~´¡!¿?\'()=&amp;%$#º°ª¬¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
    # Reemplazar caracteres especiales por letras
    temp_rep = temp.upper().replace("Ã‘","N").replace("Ã‰","E").replace("Ã“","O").replace("Ãº","U")\
    .replace("Ã­","I").replace("Ã±","N").replace("‰","A").replace("¢","O").replace("Ã³","O")\
    .replace("/"," ").replace("-"," ").replace("_"," ")
    # Eliminar acentos de letras Ej: Ã -> A 
    final = unidecode(temp_rep)

    return final


def ttelefono(columna_telefono):
    return F.when(F.length(columna_telefono) < 2, "").otherwise(
        F.translate(
            F.upper(
                F.when(F.instr(columna_telefono, ",") != 0, F.split(columna_telefono, ",", 2).getItem(1))
                .otherwise(
                    F.when(F.instr(columna_telefono, ";") != 0, F.split(columna_telefono, ";", 2).getItem(1))
                    .otherwise(
                        F.when(F.instr(columna_telefono, "/") != 0, F.split(columna_telefono, "/", 2).getItem(1))
                        .otherwise(
                            F.when(F.instr(columna_telefono, "Y") != 0, F.split(columna_telefono, "Y", 2).getItem(1))
                            .otherwise(
                                F.when(F.instr(columna_telefono, "EXT") != 0, F.split(columna_telefono, "EXT", 2).getItem(1))
                                .otherwise(columna_telefono)
                            )
                        )
                    )
                )
            ),
            "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ",
            ""
        ))

def tcelular(columna_celular):
    return F.when(F.length(columna_celular) < 2, "").otherwise(
        F.translate(
            F.upper(
                F.when(F.instr(columna_celular, ",") != 0, F.split(columna_celular, ",", 2).getItem(1))
                .otherwise(
                    F.when(F.instr(columna_celular, ";") != 0, F.split(columna_celular, ";", 2).getItem(1))
                    .otherwise(
                        F.when(F.instr(columna_celular, "/") != 0, F.split(columna_celular, "/", 2).getItem(1))
                        .otherwise(
                            F.when(F.instr(columna_celular, "Y") != 0, F.split(columna_celular, "Y", 2).getItem(1))
                            .otherwise(
                                F.when(F.instr(columna_celular, "EXT") != 0, F.split(columna_celular, "EXT", 2).getItem(1))
                                .otherwise(columna_celular)
                            )
                        )
                    )
                )
            ),
            "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ",
            ""
        ))