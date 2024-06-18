from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c
import logging

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def refoep01(spark, datasource, columnas, client, branch, report):
    # Load defined functions
    LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())
    LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

    # Read the import.csv file to get column names
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in imp.collect()]

    # Read and process data from the source file
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]
    data = data.selectExpr(*expresiones)
    
    # Apply transformations
    data = (data
            .withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.current_date())
            .withColumn("NumeroOT", LTexto(F.col("NumeroOT")))
            .withColumn("Taller", LTexto(F.col("Taller")))
            .withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
            .withColumn("FechaEntrega", F.to_date(F.col("FechaEntrega"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("NumeroParte", LTexto(F.substring(F.col("NumeroParte").cast("string"), 1, 20)))
            .withColumn("Descripcion", LTexto(F.substring(F.col("Descripcion").cast("string"), 1, 30)))
            .withColumn("Cantidad", F.col("Cantidad"))
            .withColumn("`VentaUnit$`", F.col("`VentaUnit$`").cast(DecimalType(18, 4)))
            .withColumn("`Venta$`", F.col("`Venta$`").cast(DecimalType(35, 10)))
            .withColumn("`CostoUnit$`", F.col("`CostoUnit$`").cast(DecimalType(18, 4)))
            .withColumn("`Costo$`", F.col("`Costo$`").cast(DecimalType(35, 10)))
            .withColumn("`Utilidad$`", F.col("`Utilidad$`").cast(DecimalType(18, 4)))
            .withColumn("Margen", F.col("Margen").cast(DecimalType(15, 4)))
           )

    # Read export.csv to get export column names
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Select and export necessary columns
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("NumeroOT") != "NUMEROOT") & (F.col("NumeroOT") != ""))

    # Convert to Pandas DataFrame
    logger.info("********** Transforming to Pandas DataFrame **********")
    pandas_df = export.toPandas()
    logger.info("Conversion Complete")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Database connection
    c.cbase(client)
    cnn = c.conect(client)

    # Drop existing data and create new database structure
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    # Insert data into database
    logger.info(f"********** Adding records to {report} for {client} **********")
    cursor = cnn.cursor()
    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    values = Exp.first()["cadena"]
    values2 = Exp2.first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        logger.info("********** Records added successfully **********")
    except Exception as e:
        cnn.rollback()
        logger.error(f"********** Error adding records: {e} **********")

    # Apply changes and export data
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)

    logger.info(f"******************************* Processing complete for {report} {branch} for client: {client} ***********************************")
