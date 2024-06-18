import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DateType, StringType
from Funciones import paths as p, Sesion as s, Limpia2 as L
import datetime, findspark, os, sys

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

################################## SE LEVANTA EL SERVIDOR DE SPARK PARA LA EJECUCION DEL MODELO ####################
logger.info(datetime.datetime.now())
# Inicializar una nueva sesión de Spark

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('load_session').getOrCreate()

# Cargar el archivo Parquet que contiene la sesión guardada
loaded_spark_df = spark.read.parquet("spark_session.parquet")

# Ahora `loaded_spark_df` es un DataFrame que contiene la sesión guardada.
# Para obtener la sesión de Spark, simplemente utiliza `getOrCreate()`
loaded_spark = SparkSession.builder.appName('loaded_session').getOrCreate()

# Verificar que la sesión se haya cargado correctamente
logger.info(f'Session : {loaded_spark.sparkContext.appName}')

####### Leer banderas ######

client = p.bclient()
branch = p.bbranch()
logger.info(f"Cliente: {client} Branch: {branch}")

#########layouts##############################

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

if branch == "05":
    logger.info("prueba")
else:
    #####procesamiento
    model = '01'
    ruta = r'/_312'

    #layouts=["invnue","invusa","refinv","refoep","seroep","sertec","vtanue","vtausa","refmos","refser"]
    layouts = ["invnue","invusa","refinv","refoep","seroep","sertec","refser"]
    #layouts = ["refinv"]

######## For Interacción de Layouts#######
    for i in layouts:
        logger.info(f"Procesando Layouts {i}.......")

        if i == "prop":
            ruta = r'/QuiterM'

        if i == "prop":
            model = '01'

        report = i + model
        layout = i + branch

        try:
            path = p.path(layout)
            pathImp = p.pathimp(report, ruta)

            ###### Condicionales de Layouts #########
            logger.info(f"Enviando a Procesamiento {report}")
            project = getattr(sys.modules[__name__], report)
            project(spark, path, pathImp, client, branch, i)

        except Exception as e:
            logger.error(f"Error: {e}")

#spark.stop()
logger.info(f"Sesion {client}{branch} Finalizada")
#L.limpia2()
#c.dbase(client)
