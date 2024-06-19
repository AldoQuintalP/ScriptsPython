from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c
import logging

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def refser01(spark, datasource, columnas, client, branch, report):
    logger.info("Inicia el proceso de refser01 para el cliente: %s, sucursal: %s, y reporte: %s", client, branch, report)

    # Carga funciones definidas
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
    data = (data
            .withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.lit(F.current_date()))
            .withColumn("Factura", LTexto(F.col("Factura")))
            .withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("NumeroParte", LTexto(F.col("NumeroParte")))
            .withColumn("Descripcion", LTexto(F.substring(F.col("Descripcion").cast("string"), 1, 30)))
            .withColumn("Cantidad", F.col("Cantidad"))
            .withColumn("`VentaUnit$`", F.coalesce(F.col("`VentaUnit$`"), F.lit(0)).cast(DecimalType(18, 4)))
            .withColumn("`Venta$`", F.coalesce(F.col("`Venta$`"), F.lit(0)).cast(DecimalType(35, 10)))
            .withColumn("`CostoUnit$`", F.coalesce(F.col("`CostoUnit$`"), F.lit(0)).cast(DecimalType(18, 4)))
            .withColumn("`Costo$`", F.coalesce(F.col("`Costo$`"), F.lit(0)).cast(DecimalType(35, 10)))
            .withColumn("`Utilidad$`", F.coalesce(F.col("`Utilidad$`"), F.lit(0)).cast(DecimalType(18, 4)))
            .withColumn("Margen", 
                        F.when((F.col("`Costo$`") == 0) | (F.col("`Venta$`") == 0), 0)
                        .otherwise(F.when(F.col("`Venta$`") < 0, ((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * -100)
                        .otherwise(((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * 100)))
            .withColumn("RFC", LCodigos(F.substring(F.col("RFC").cast("string"), 1, 13)))
            .withColumn("NumeroOT", LTexto(F.col("NumeroOT")))
            .withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
            .withColumn("Taller", LTexto(F.col("Taller"))))

    # Lee las columnas desde el archivo exportar.csv
    nombresColumnasExp = spark.read.option("header", True).csv(columnas).rdd.map(lambda row: row[3]).collect()

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("Factura") != "FACTURA") & (F.col("Factura") != ""))

    ########################################## Se pasa el archivo a DF y se obtienen los meses actualizados ###################
    logger.info("Transformando a Pandas DF...")
    pandas_df = export.toPandas()
    logger.info("Conversion Finalizada")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    #################################### SE LLAMA LA CONEXION CON LA BASE DE DATOS #################################
    c.cbase(client)
    cnn = c.conect(client)

    ################################### COMIENZA PROCESO DE BORRADO DE INFORMACION ##################################
    s.drop(report, branch, client, cnn)

    ################################### COMIENZA PROCESO DE CREACION DE DB ##################################
    s.create(columnas, report, branch, cnn)

    ############################## INSERCIÓN DE DATOS ######################################
    logger.info("Agregando registros...")
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
    except:
        cnn.rollback()
        logger.error("Error al cargar los registros")
    
    ############################## INSERCIÓN DE DATOS ######################################
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    logger.info("Finaliza Procesamiento %s%s Del Cliente: %s", report, branch, client)
