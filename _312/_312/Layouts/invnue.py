import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def invnue01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    LTexto = F.udf(FE.LimpiaTexto, StringType())

    # Lee el archivo import.csv para obtener los nombres de las columnas
    nombresColumnas = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[1]).collect()

    # Lee y procesa los datos desde el archivo fuente
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    data = data.selectExpr(*[f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)])

    # Realiza las transformaciones en cadena
    data = (data
        .withColumn("Client", F.lit(client))
        .withColumn("Branch", F.lit(branch))
        .withColumn("Date", F.current_date())
        .withColumn("Vin", LTexto(F.substring(F.col("Vin").cast("string"), 1, 20)))
        .withColumn("NumeroInventario", LTexto(F.substring(F.col("NumeroInventario").cast("string"), 1, 10)))
        .withColumn("Ano", LTexto(F.col("Ano")))
        .withColumn("Marca", LTexto(F.substring(F.col("Marca").cast("string"), 1, 10)))
        .withColumn("Modelo", LTexto(F.substring(F.col("Modelo").cast("string"), 1, 30)))
        .withColumn("Version", LTexto(F.substring(F.col("Version").cast("string"), 1, 15)))
        .withColumn("Color", LTexto(F.col("Color")))
        .withColumn("Interior", LTexto(F.col("Interior")))
        .withColumn("`Costo$`", F.col("`Costo$`").cast(DecimalType(18, 2)))
        .withColumn("FechaCompra", F.to_date(F.col("FechaCompra"), "MM/dd/yyyy").cast(DateType()))
        .withColumn("Dias", F.when(F.isnull(F.col("FechaCompra")), 0).otherwise(F.datediff(F.current_date(), F.col("FechaCompra"))))
        .withColumn("Status", LTexto(F.col("Status")))
        .withColumn("TipoCompra", LTexto(F.col("TipoCompra"))))

    # Lee las columnas desde el archivo exportar.csv
    nombresColumnasExp = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[3]).collect()

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select(*[F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("Vin") != "VIN") & (F.col("Vin") != ""))

    # Convertir a Pandas DataFrame
    logger.info("Transformando a Pandas DataFrame")
    pandas_df = export.toPandas()
    logger.info("Conversión finalizada")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Conexión con la base de datos
    c.cbase(client)
    cnn = c.conect(client)

    # Proceso de borrado y creación de la base de datos
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    # Inserción de datos
    logger.info(f"Agregando registros... {report} {branch} a la base: sim_{client}")
    cursor = cnn.cursor()

    expor = spark.read.option("header", True).csv(columnas)
    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena")).first()["cadena"]
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2")).first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({Exp}) VALUES ({Exp2})"

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
    logger.info(f"Finaliza procesamiento {report} {branch} del cliente: {client}")
