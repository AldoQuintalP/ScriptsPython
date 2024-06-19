import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sertec01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    logger.info("Inicia sertec01")
    LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())
    LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

    # Lee el archivo import.csv para obtener los nombres de las columnas
    nombresColumnas = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[1]).collect()


    # Lee y procesa los datos desde el archivo fuente
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    data = data.selectExpr(*[f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)])

    # Realiza las transformaciones en cadena
    data = (data.withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.current_date().cast(DateType()))
            .withColumn("NumeroOT", LTexto(F.substring(F.col("NumeroOT").cast("string"), 1, 10)))
            .withColumn("CodigoOperacion", LTexto(F.col("CodigoOperacion")))
            .withColumn("FechaCierre", F.to_date(F.col("FechaCierre"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("Taller", LTexto(F.col("Taller")))
            .withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
            .withColumn("HorasPagadas", F.col("HorasPagadas").cast(DecimalType(18, 2)))
            .withColumn("HorasFacturadas", F.col("HorasFacturadas").cast(DecimalType(18, 2)))
            .withColumn("NumeroMecanico", LTexto(F.col("NumeroMecanico")))
            .withColumn("NombreMecanico", LTexto(F.col("NombreMecanico"))))

    # Lee las columnas desde el archivo exportar.csv
    nombresColumnasExp = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[3]).collect()

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("NumeroOT") != "NUMEROOT") & (F.col("NumeroOT") != ""))

    # Conversión a Pandas DataFrame
    logger.info("Transformando a Pandas DataFrame")
    pandas_df = export.toPandas()
    logger.info("Conversión a Pandas DataFrame finalizada")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Conexión a la base de datos
    c.cbase(client)
    cnn = c.conect(client)

    # Proceso de borrado y creación de la base de datos
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    # Inserción de datos
    logger.info(f"Agregando registros... {report}{branch} a la base: sim_{client}")
    cursor = cnn.cursor()

    expor = spark.read.option("header", True).csv(columnas)
    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    values = Exp.first()["cadena"]
    values2 = Exp2.first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        logger.info("Registros agregados correctamente")
    except Exception as e:
        cnn.rollback()
        logger.error(f"Error al cargar los registros: {e}")

    # Actualización y exportación de datos
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    logger.info(f"Finaliza procesamiento {report}{branch} del cliente: {client}")
