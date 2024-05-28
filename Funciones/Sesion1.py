from pyspark.sql import SparkSession
import findspark

# Inicializar findspark
findspark.init()

# Variable global para almacenar la sesión de Spark
try:
    spark
    print(" ########## Try Spark #############")
except NameError:
    spark = None

def get_spark_session():
    global spark
    print(f'SPARk ............................... {spark}')
    # Verificar si la sesión de Spark ya está creada
    if spark is not None:
        print("Sesión de Spark ya creada. Devolviendo la sesión existente.")
        print(spark.sparkContext.appName)
        return spark
    else:
        # Si la sesión de Spark no está creada, crear una nueva
        print("*****Iniciando Proceso: Creando sesión de SPARK*****")
        spark = SparkSession.builder.appName('firstsession') \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "1g") \
            .config('spark.master', 'local[4]') \
            .config('spark.shuffle.sql.partitions', 1) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        # Guardar la sesión de Spark en formato parquet
        spark.catalog.clearCache()  # Limpiar la caché antes de guardar
        # Crear un DataFrame a partir de una lista de tuplas
        data = [("Sesión de Spark guardada en formato Parquet",)]
        spark.createDataFrame(data, ["mensaje"]).write.mode("overwrite").parquet("spark_session.parquet")

        print(f'Spark ________________________ {spark}')
        
        return spark

# Obtener la sesión de Spark
# spark = get_spark_session()
