import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def vtanue01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    logger.info("Inicia vtanue01")
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
            .withColumn("Factura", LCodigos(F.col("Factura")))
            .withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("TipoVenta", LTexto(F.col("TipoVenta")))
            .withColumn("TipoPago", LTexto(F.col("TipoPago")))
            .withColumn("Vin", LTexto(F.substring(F.col("Vin").cast("string"), 1, 20)))
            .withColumn("NumeroInventario", LCodigos(F.col("NumeroInventario")))
            .withColumn("`Isan$`", F.when(F.col("`Isan$`").isNotNull(), F.col("`Isan$`")).otherwise(0).cast(DecimalType(18, 4)))
            .withColumn("`Costo$`", F.when(F.col("`Costo$`").isNotNull(), F.col("`Costo$`")).otherwise(0).cast(DecimalType(35, 10)))
            .withColumn("`Venta$`", F.when(F.col("`Venta$`").isNotNull(), F.col("`Venta$`")).otherwise(0).cast(DecimalType(35, 10)))
            .withColumn("`Utilidad$`", F.when(F.col("`Utilidad$`").isNotNull(), F.col("`Venta$`")).otherwise(0).cast(DecimalType(18, 4)))
            .withColumn("Margen", 
                       F.when((F.col("`Costo$`") == 0) | (F.col("`Venta$`") == 0), 0)
                       .otherwise(F.when(F.col("`Venta$`") < 0, ((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * -100)
                                   .otherwise(((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * 100)))
            .withColumn("Ano", LTexto(F.col("Ano")))
            .withColumn("Marca", LTexto(F.col("Marca")))
            .withColumn("Modelo", LTexto(F.substring(F.col("Modelo").cast("string"), 1, 30)))
            .withColumn("Color", LTexto(F.col("Color")))
            .withColumn("Interior", LTexto(F.col("Interior")))
            .withColumn("NumeroVendedor", LTexto(F.col("NumeroVendedor")))
            .withColumn("NombreVendedor", LTexto(F.substring(F.col("NombreVendedor").cast("string"), 1, 30)))
            .withColumn("FechaCompra", F.to_date(F.col("FechaCompra"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("FechaEntrega", F.to_date(F.col("FechaEntrega"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("NombreCliente", LTexto(F.substring(F.col("NombreCliente").cast("string"), 1, 30)))
            .withColumn("RFC", LCodigos(F.substring(F.col("RFC").cast("string"), 1, 13)))
            .withColumn("Direccion", LTexto(F.col("Direccion")))
            .withColumn("Telefono", LCodigos(F.col("Telefono")))
            .withColumn("CP", LTexto(F.col("CP")))
            .withColumn("Email", LEmail(F.col("Email")))
            .withColumn("VentasNetas", 
                        F.when((F.col("`Venta$`") == 0) | (F.col("`Costo$`") == 0), 0)
                        .when((F.col("`Venta$`") < 0) | (F.col("`Costo$`") < 0), -1)
                        .otherwise(1)))

    # Lee las columnas desde el archivo exportar.csv
    nombresColumnasExp = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[3]).collect()
 
    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("Vin") != "VIN") & (F.col("Vin") != ""))

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
