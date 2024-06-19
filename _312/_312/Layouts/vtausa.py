import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c
import os
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def vtausa01(spark, datasource, columnas, client, branch, report):

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Load defined functions
    LCodigos = F.udf(FE.LimpiaCodigos, StringType())
    LTexto = F.udf(FE.LimpiaTexto, StringType())
    LEmail = F.udf(FE.LimpiaEmail, StringType())

    # Read import.csv to get column names
    nombresColumnas = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[1]).collect()

    # Read and process data from the source file
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    data = data.selectExpr(*[f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)])

    # Apply transformations
    data = (data
            .withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.current_date())
            .withColumn("Factura", LCodigos(F.col("Factura")))
            .withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("TipoVenta", LTexto(F.col("TipoVenta")))
            .withColumn("TipoPago", LTexto(F.col("TipoPago")))
            .withColumn("Vin", F.substring(F.col("Vin").cast("string"), 1, 20))
            .withColumn("Vin", LTexto(F.col("Vin")))
            .withColumn("NumeroInventario", LCodigos(F.col("NumeroInventario")))
            .withColumn("`Isan$`", F.coalesce(F.col("`Isan$`"), F.lit(0)).cast(DecimalType(18, 4)))
            .withColumn("`Costo$`", F.coalesce(F.col("`Costo$`"), F.lit(0)).cast(DecimalType(35, 10)))
            .withColumn("`Venta$`", F.coalesce(F.col("`Venta$`"), F.lit(0)).cast(DecimalType(35, 10)))
            .withColumn("`Utilidad$`", F.coalesce(F.col("`Venta$`"), F.lit(0)).cast(DecimalType(18, 4)))
            .withColumn("Margen", 
                        F.when((F.col("`Costo$`") == 0) | (F.col("`Venta$`") == 0), 0)
                        .otherwise(F.when(F.col("`Venta$`") < 0, ((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * -100)
                        .otherwise(((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * 100))
                       )
            .withColumn("Ano", LTexto(F.col("Ano")))
            .withColumn("Marca", LTexto(F.col("Marca")))
            .withColumn("Modelo", F.substring(F.col("Modelo").cast("string"), 1, 30))
            .withColumn("Modelo", LTexto(F.col("Modelo")))
            .withColumn("Color", LTexto(F.col("Color")))
            .withColumn("Interior", LTexto(F.col("Interior")))
            .withColumn("NumeroVendedor", LTexto(F.col("NumeroVendedor")))
            .withColumn("NombreVendedor", F.substring(F.col("NombreVendedor").cast("string"), 1, 30))
            .withColumn("NombreVendedor", LTexto(F.col("NombreVendedor")))
            .withColumn("FechaCompra", F.to_date(F.col("FechaCompra"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("FechaEntrega", F.to_date(F.col("FechaEntrega"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("NombreCliente", F.substring(F.col("NombreCliente").cast("string"), 1, 30))
            .withColumn("NombreCliente", LTexto(F.col("NombreCliente")))
            .withColumn("RFC", F.substring(F.col("RFC").cast("string"), 1, 13))
            .withColumn("RFC", LCodigos(F.col("RFC")))
            .withColumn("Direccion", LTexto(F.col("Direccion")))
            .withColumn("Telefono", LCodigos(F.col("Telefono")))
            .withColumn("CP", LTexto(F.col("CP")))
            .withColumn("Email", LEmail(F.col("Email")))
            .withColumn("VentasNetas", 
                        F.when((F.col("`Venta$`") == 0) | (F.col("`Costo$`") == 0), 0)
                        .when((F.col("`Venta$`") < 0) | (F.col("`Costo$`") < 0), -1)
                        .otherwise(1)
                       )
           )

    # Read export.csv to get export column names
    nombresColumnasExp = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[3]).collect()

    # Select necessary columns and filter data
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("Vin") != "VIN") & (F.col("Vin") != ""))

    # Convert to Pandas DataFrame
    pandas_df = export.toPandas()
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Database connection
    c.cbase(client)
    cnn = c.conect(client)

    # Drop existing data and create new database structure
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    # Insert data into the database
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
        logger.info("********** Registros agregados correctamente **********")
    except:
        cnn.rollback()
        logger.error("********** Error al cargar los registros **********")
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    logger.info("******************************* Finaliza Procesamiento " + report + "" + branch + " Del Cliente: " + client + " ***********************************" )
