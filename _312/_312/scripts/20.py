
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DateType, StringType
from Funciones import paths as p, Sesion as s, Limpia2 as L
#from Funciones import conexion as c
import datetime, findspark,os,sys

################################## SE LEVANTA EL SERVIDOR DE SPARK PARA LA EJECUCION DEL MODELO ####################
print(datetime.datetime.now())
# # Inicializar una nueva sesión de Spark

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('load_session').getOrCreate()
# print(f'Spark: {spark}')


# # Cargar el archivo Parquet que contiene la sesión guardada
loaded_spark_df = spark.read.parquet("spark_session.parquet")

# # Ahora `loaded_spark_df` es un DataFrame que contiene la sesión guardada.
# # Para obtener la sesión de Spark, simplemente utiliza `getOrCreate()`
loaded_spark = SparkSession.builder.appName('loaded_session').getOrCreate()

# # Verificar que la sesión se haya cargado correctamente
#print(f'Session : {loaded_spark.sparkContext.appName}')

####### Leer banderas ######N


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

from _312._312.Layouts.invnue import *
from _312._312.Layouts.seroep import *
from _312._312.Layouts.refcom import *
from _312._312.Layouts.refinv import *
from _312._312.Layouts.invusa import *
from _312._312.Layouts.refoep import *
from _312._312.Layouts.vtanue import *
from _312._312.Layouts.vtausa import *
from _312._312.Layouts.sertec import *
from _312._312.Layouts.refmos import *
from _312._312.Layouts.refser import *


#from QuiterM.Layouts.refmos import *
# from QuiterM.Layouts.refinv import *
# from QuiterM.Layouts.vtanue import *
# from QuiterM.Layouts.vtausa import *
# from QuiterM.Layouts.invnue import *
# from QuiterM.Layouts.invusa import *
# from SICOP.Layouts.prospe import *

if branch == "05":
    print("prueba")

else:
    #####procesamiento
    model = '01'
    ruta = r'/_312'

    #layouts=["invnue","invusa","refcom","refinv","refoep","seroep","sertec","vtanue","vtausa","refmos","refser"]
    layouts=["refser"]

######## For Interacción de Layouts#######
    for i in layouts:
        print("Procesando Layouts " + i + ".......")
  
        if i == "prop":
            ruta = r'/QuiterM'
    
        if i == "prop":
            model = '01'
    
        report = i + model
        #print(f'Report: {report}')
        layout = i + branch
        #print(f'Layout: {layout}')

        try:
            path = p.path(layout)
            #print(f'Path: {path}')
            pathImp= p.pathimp(report,ruta)
            #print(f'PathImp: {pathImp}')
            #print(pathImp)
            #pathExp= p.pathexp(report,ruta)

        ###### Condicionales de Layouts #########
            print("Enviando a Procesamiento " + report)
        ###convierte el report a funcion ####
            project = getattr(sys.modules[__name__],report)
            print(f'Project ... {project}')
            print(f'Client ... {client}')
            print(f'Branch ... {branch}')
            print(f'i ... {i}')
            print(f'Spark: {spark}')
            print(f'Path .. {path}')
            print(f'Path imp: {pathImp}')
            project(spark,path,pathImp,client,branch,i)
            
        except Exception as e:
            print("Error: ",e)       


#spark.stop()
#spark
print("Sesion " + client + branch + " Finalizada")
#L.limpia2()
#c.dbase(client)