import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, FloatType
from Funciones import FuncionesExternas as FE
from Funciones import conexion as C, Sentencias as s
import pandas as pd
import mysql.connector

# Configure logger with timestamp and log level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def refcom01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())

    # Lee el archivo de texto y el archivo import.csv
    imp = spark.read.option("header", True).csv(columnas)
    data = spark.read.text(datasource)

    # Divide cada línea en columnas utilizando el carácter "|"
    data = data.withColumn("columns", F.split(data["value"], "\\|"))

    # Lee el nombre de las columnas del archivo import.csv
    nombresColumnas = [row[1] for row in imp.collect()]
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]

    # Actualiza el nombre de las columnas
    data = data.selectExpr(*expresiones)

    # Columnas runtime
    data = data.withColumn("Client", F.lit(client))
    data = data.withColumn("Branch", F.lit(branch))
    data = data.withColumn("Date", F.current_date())

    # Aplica LimpiaTexto y LimpiaCodigo a las columnas correspondientes
    data = data.withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yy").cast(DateType()))
    data = data.withColumn("Factura", LCodigos(F.col("Factura")))
    data = data.withColumn("NumeroProveedor", LCodigos(F.col("NumeroProveedor")))
    data = data.withColumn("TipoProveedor", LTexto(F.col("TipoProveedor")))
    data = data.withColumn("NombreProveedor", LTexto(F.substring(F.col("NombreProveedor").cast("string"), 1, 30)))
    data = data.withColumn("TipoCompra", LTexto(F.col("TipoCompra")))
    data = data.withColumn("NumeroParte", LTexto(F.col("NumeroParte")))
    data = data.withColumn("Descripcion", LTexto(F.substring(F.col("Descripcion").cast("string"), 1, 30)))
    data = data.withColumn("`CostoUnit$`", F.abs(F.col("`CostoUnit$`")))
    data = data.withColumn("Cantidad", F.col("Cantidad"))
    data = data.withColumn("`Costo$`", F.col("`Costo$`"))

    # Lee las columnas desde el archivo exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("Factura") != "FACTURA") & (F.col("Factura") != "") & (F.col("FechaFactura").isNotNull()))
    export.show()

    ########################################## Se pasa el archivo a DF y se obtienen los meses actualizados ###################
    logger.info("********** Transformado a Pandas DF **********")
    pandas_df = export.toPandas()
    logger.info("ConversionFinalizada")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    #################################### SE LLAMA LA CONEXION CON LA BASE DE DATOS #################################
    C.cbase(client)
    cnn = C.conect(client)

    ################################### COMIENZA PROCESO DE BORRADO DE INFORMACION ##################################
    s.drop(report, branch, client, cnn)

    ################################### COMIENZA PROCESO DE CREACION DE DB ##################################
    s.create(columnas, report, branch, cnn)

    ############################## INSERCIÓN DE DATOS ######################################
    logger.info(f"********** Agregando registros... {report} a la base: {client} **********")
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

    ############################## INSERCIÓN DE DATOS ######################################
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    
    logger.info(f"******************************* Finaliza Procesamiento {report} {branch} Del Cliente: {client} ***********************************")
