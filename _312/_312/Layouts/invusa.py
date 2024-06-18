import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, DecimalType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

# Configure logger with timestamp and log level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def invusa01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    LCodigos = F.udf(lambda z: FE.LimpiaCodigosv1(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTextov1(z), StringType())
    LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

    # Lee el archivo import.csv para obtener los nombres de las columnas
    encabezados = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in encabezados.collect()]

    # Lee y procesa los datos desde el archivo fuente omitiendo las dos primeras líneas
    rdd = spark.sparkContext.textFile(datasource).zipWithIndex().filter(lambda x: x[1] >= 2).map(lambda x: x[0])

    # Crea un DataFrame a partir del RDD y selecciona solo la segunda columna
    data = spark.createDataFrame(rdd.map(lambda x: (x,)), ["value"]).withColumn("columns", F.split(F.col("value"), "\\|"))
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]
    data = data.selectExpr(*expresiones).select(data.columns[1:]).drop("None")

    # Muestra el DataFrame resultante
    data.show()
    data.printSchema()

    # Realiza las transformaciones en cadena
    data_n = data.withColumn("Client", F.lit(client).cast(IntegerType())) \
                 .withColumn("Branch", F.lit(branch).cast(IntegerType())) \
                 .withColumn("Date", F.current_date()) \
                 .withColumn("Vin", LTexto(F.substring(F.col("Vin").cast("string"), 1, 20))) \
                 .withColumn("NumeroInventario", LTexto(F.substring(F.col("NumeroInventario").cast("string"), 1, 10))) \
                 .withColumn("Ano", F.col("Ano").cast(IntegerType())) \
                 .withColumn("Marca", LTexto(F.substring(F.col("Marca").cast("string"), 1, 10))) \
                 .withColumn("Modelo", LTexto(F.substring(F.col("Modelo").cast("string"), 1, 30))) \
                 .withColumn("Version", LTexto(F.substring(F.col("Version").cast("string"), 1, 15))) \
                 .withColumn("Color", LTexto(F.col("Color"))) \
                 .withColumn("Interior", LTexto(F.col("Interior"))) \
                 .withColumn("`Costo$`", F.col("`Costo$`").cast(DecimalType(18, 2))) \
                 .withColumn("`Isan$`", F.col("`Isan$`").cast(DecimalType(18, 2))) \
                 .withColumn("`CostoCompra$`", F.col("`CostoCompra$`").cast(DecimalType(18, 2))) \
                 .withColumn("FechaCompra", F.to_date(F.col("FechaCompra"), "dd/MM/yyyy").cast(DateType())) \
                 .withColumn("Status", LTexto(F.col("Status"))) \
                 .withColumn("TipoCompra", LTexto(F.col("TipoCompra"))) \
                 .withColumn("Dias", F.datediff(F.current_date(), F.col("FechaCompra")))

    # Lee las columnas desde el archivo exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Selecciona las columnas necesarias y exporta los datos
    export = data_n.select([F.col(columna) for columna in nombresColumnasExp]).filter((F.col("Vin") != "VIN") & (F.col("Vin") != ""))
    export.show()

    ########################################## Se pasa el archivo a DF y se obtienen los meses actualizados ###################
    logger.info("********** Transformado a Pandas DF **********")
    pandas_df = export.toPandas()
    logger.info("ConversionFinalizada")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    #################################### SE LLAMA LA CONEXION CON LA BASE DE DATOS #################################
    c.cbase(client)
    cnn = c.conect(client)

    ################################### COMIENZA PROCESO DE BORRADO DE INFORMACION ##################################
    s.drop(report, branch, client, cnn)

    ################################### COMIENZA PROCESO DE CREACION DE DB ##################################
    s.create(columnas, report, branch, cnn)

    ############################## INSERCIÓN DA DATOS ######################################
    logger.info(f"********** Agregando registros... {report} {branch} a la base: sim_{client} **********")
    cursor = cnn.cursor()

    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    values = Exp.first()["cadena"]
    values2 = Exp2.first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        logger.info("********** Registros agregados correctamente **********")
    except Exception as e:
        cnn.rollback()
        logger.error(f"********** Error al cargar los registros: {e} **********")

    ############################## INSERCIÓN DA DATOS ######################################
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    logger.info(f"******************************* Finaliza Procesamiento {report} {branch} Del Cliente: {client} ***********************************")
