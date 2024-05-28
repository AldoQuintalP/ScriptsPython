
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DateType, StringType
from Funciones import paths as p, Sesion as s, Limpia2 as L
from Funciones import conexion as c
import datetime, findspark,os,sys


################################## SE LEVANTA EL SERVIDOR DE SPARK PARA LA EJECUCION DEL MODELO ####################
print(datetime.datetime.now())
findspark.init()
spark = s.get_spark_session()

####### Leer banderas ######

client=p.bclient()
branch=p.bbranch()
print("Cliente: " + client + " Branch: " + branch)

#########layouts##############################

# Obtén la ruta absoluta de la carpeta que contiene el script 20.py
#script_dir = os.getcwd()
# Agrega la ruta de la carpeta 'Layouts' al sys.path
#layouts_dir = os.path.join(script_dir, client,"Layouts")
#sys.path.append(layouts_dir)
#print(layouts_dir)

from _312.Layouts.invnue import *
from _312.Layouts.seroep import *
from _312.Layouts.refcom import *
from _312.Layouts.refinv import *
from _312.Layouts.invusa import *
from _312.Layouts.refoep import *
from _312.Layouts.vtanue import *
from _312.Layouts.vtausa import *
from _312.Layouts.sertec import *
from _312.Layouts.refmos import *
from _312.Layouts.refser import *


#from QuiterM.Layouts.refmos import *
from QuiterM.Layouts.refinv import *
from QuiterM.Layouts.vtanue import *
from QuiterM.Layouts.vtausa import *
from QuiterM.Layouts.invnue import *
from QuiterM.Layouts.invusa import *
from SICOP.Layouts.prospe import *

if branch == "05":
    print("prueba")

else:
    #####procesamiento
    model = '01'
    ruta = r'/_312'

    layouts=["invusa"]
    #layouts=["invnue","invusa","refcom","refinv","refoep","seroep","sertec","vtanue","vtausa","refmos","refser"]

######## For Interacción de Layouts#######
    for i in layouts:
        print("Procesando Layouts " + i + ".......")
  
        if i == "prop":
            ruta = r'/QuiterM'
    
        if i == "prop":
            model = '01'
    
        report = i + model
        layout = i + branch
        try:
            path = p.path(layout)
            pathImp= p.pathimp(report,ruta)
            #print(pathImp)
            #pathExp= p.pathexp(report,ruta)

        ###### Condicionales de Layouts #########
            print("Enviando a Procesamiento " + report)
        ###convierte el report a funcion ####
            project = getattr(sys.modules[__name__],report)
            project(spark,path,pathImp,client,branch,i)
        except Exception as e:
            print("Error: ",e)       


#spark.stop()
#spark
print("Sesion " + client + branch + " Finalizada")
#L.limpia2()
#c.dbase(client)