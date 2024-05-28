from pyspark.sql import SparkSession
import findspark

def get_spark_session():
    if 'spark' in globals():

        print("Iniciando Proceso Reutilizando sesión de SPARK")

        return globals()['spark']
    else:
        print("Iniciando Proceso Creando sesión de SPARK")
       
        spark= SparkSession.builder.appName('firstsession')\
            .config('spark.master','local[4]')\
            .config('spark.shuffle.sql.partitions',1)\
            .getOrCreate()
        
        '''   
        spark = SparkSession.builder.appName("MyApp").getOrCreate()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        spark.conf.set("spark.sql.sessionEncoding", "UTF-8")
        '''
    
        globals()['spark'] = spark
        return spark