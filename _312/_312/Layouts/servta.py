from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, IntegerType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def refser01(spark, datasource, columnas, client, branch, report):
    logging.info("Inicio del procesamiento de servta01")
    # Load user-defined functions
    LCodigos = F.udf(FE.LimpiaCodigos, StringType())
    LTexto = F.udf(FE.LimpiaTexto, StringType())
    LEmail = F.udf(FE.LimpiaEmail, StringType())

    # Read column names from import.csv
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in imp.collect()]

    # Read and process data from the source file
    data = spark.read.text(datasource).withColumn("columns", F.split("value", "\\|"))
    data = data.select([F.col(f"columns[{i}]").alias(nombresColumnas[i]) for i in range(len(nombresColumnas))])

    # Add new columns and perform transformations
    data = (
        data.withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.current_date())
            .withColumn("FechaFactura", F.to_date("FechaFactura", "dd/MM/yy").cast(DateType()))
            .withColumn("FechaApertura", F.to_date("FechaApertura", "dd/MM/yy").cast(DateType()))
            .withColumn("Dias", F.datediff("FechaFactura", "FechaApertura"))
            .withColumn("Factura", LCodigos("Factura"))
            .withColumn("Taller", F.when(F.instr("TipoOrden", "HYP") != 0, "HYP")
                                   .when(F.instr("TipoOrden", "LYP") != 0, "HYP")
                                   .when(F.instr("TipoOrden", "CARRO") != 0, "HYP")
                                   .when(F.instr("TipoOrden", "SEGUR") != 0, "HYP")
                                   .otherwise("SERVICIO"))
            .withColumn("TipoOrden", LTexto("TipoOrden"))
            .withColumn("TipoPago", LTexto("TipoPago"))
            .withColumn("NumeroOT", LCodigos("NumeroOT"))
            .withColumn("CodigoOperacion", LCodigos("CodigoOperacion"))
            .withColumn("Descripcion", LTexto("Descripcion"))
            .withColumn("VentaMO", F.col("VentaMO").cast(DecimalType()))
            .withColumn("DescuentoMO", F.col("DescuentoMO").cast(DecimalType()))
            .withColumn("CostoMO", F.col("CostoMO").cast(DecimalType()))
            .withColumn("VentaMateriales", F.col("VentaMateriales").cast(DecimalType()))
            .withColumn("DescuentoMateriales", F.col("DescuentoMateriales").cast(DecimalType()))
            .withColumn("CostoMateriales", F.col("CostoMateriales").cast(DecimalType()))
            .withColumn("VentaTOT", F.col("VentaTOT").cast(DecimalType()))
            .withColumn("DescuentoTOT", F.col("DescuentoTOT").cast(DecimalType()))
            .withColumn("CostoTOT", F.col("CostoTOT").cast(DecimalType()))
            .withColumn("VentaPartes", F.col("VentaPartes").cast(DecimalType()))
            .withColumn("DescuentoPartes", F.col("DescuentoPartes").cast(DecimalType()))
            .withColumn("CostoPartes", F.col("CostoPartes").cast(DecimalType()))
            .withColumn("Venta", (F.col("VentaMO") + F.col("VentaMateriales") + F.col("VentaTOT")).cast(DecimalType()))
            .withColumn("Costo", (F.col("CostoMO") + F.col("CostoMateriales") + F.col("CostoTOT")).cast(DecimalType()))
            .withColumn("VentaTotal", F.col("VentaTotal").cast(DecimalType()))
            .withColumn("CostoTotal", F.col("CostoTotal").cast(DecimalType()))
            .withColumn("VentasNetas", F.when(F.col("Venta") == 0, 0)
                                        .when(F.col("Venta") < 0, -1)
                                        .otherwise(1))
            .withColumn("Utilidad", F.when(F.col("Costo") == 0, 0)
                                     .otherwise(F.col("Venta") - F.col("Costo")).cast(DecimalType()))
            .withColumn("Margen", F.when(F.col("Costo") == 0, 0)
                                   .otherwise(((F.col("Venta") / F.col("Costo")) - 1) * 100)
                                   .cast(DecimalType()))
            .withColumn("NumeroAsesor", LCodigos("NumeroAsesor"))
            .withColumn("NombreAsesor", LTexto("NombreAsesor"))
            .withColumn("RFC", LCodigos("RFC"))
            .withColumn("NombreCliente", LTexto("NombreCliente"))
            .withColumn("Direccion", LTexto("Direccion"))
            .withColumn("Telefono", LCodigos("Telefono"))
            .withColumn("Telefono", F.when(F.length("Telefono") < 2, "")
                                     .otherwise(F.translate(F.upper("Telefono"), "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ", "")))
            .withColumn("CP", F.substring(LCodigos("CP"), 1, 5))
            .withColumn("CP", F.when(F.length("CP") < 2, "").otherwise("CP"))
            .withColumn("Email", LEmail("Email"))
            .withColumn("Odometro", F.col("Odometro").cast(IntegerType()))
            .withColumn("Vin", LCodigos("Vin"))
            .withColumn("Ano", F.col("Ano").cast(IntegerType()))
            .withColumn("Marca", LTexto("Marca"))
            .withColumn("Modelo", LTexto("Modelo"))
            .withColumn("Color", LTexto("Color"))
            .withColumn("Interior", LTexto("Interior"))
            .withColumn("FechaEntrega", F.to_date("FechaEntrega", "dd/MM/yy").cast(DateType()))
            .withColumn("TipoOperacion", LTexto("TipoOperacion"))
    )

    # Construct SERVTC

    data2 = data.groupBy(
        "FechaFactura", "FechaApertura", "FechaEntrega", "FechaCierre", "Factura", "Taller",
        "TipoOrden", "TipoPago", "NumeroOT", "NumeroAsesor", "NombreAsesor", "RFC", "NombreCliente",
        "Direccion", "Telefono", "CP", "Email", "Odometro", "Vin", "Ano", "Marca", "Modelo", "Color", "Dias"
    ).agg(
        F.sum("Venta").alias("Venta"), F.sum("Costo").alias("Costo"), F.sum("Utilidad").alias("Utilidad"),
        F.sum("Margen").alias("Margen"), F.sum("VentaMO").alias("VentaMO"), F.sum("DescuentoMO").alias("DescuentoMO"),
        F.sum("CostoMO").alias("CostoMO"), F.sum("VentaMateriales").alias("VentaMateriales"),
        F.sum("DescuentoMateriales").alias("DescuentoMateriales"), F.sum("CostoMateriales").alias("CostoMateriales"),
        F.sum("VentaTOT").alias("VentaTOT"), F.sum("DescuentoTOT").alias("DescuentoTOT"), F.sum("CostoTOT").alias("CostoTOT"),
        F.sum("VentaPartes").alias("VentaPartes"), F.sum("DescuentoPartes").alias("DescuentoPartes"), F.sum("CostoPartes").alias("CostoPartes"),
        F.sum("VentaTotal").alias("VentaTotal"), F.sum("CostoTotal").alias("CostoTotal")
    ).withColumn("Client", F.lit(client)).withColumn("Branch", F.lit(branch)).withColumn("VentasNetas", F.when(F.col("Venta") == 0, 0).otherwise(F.when(F.col("Venta") < 0, -1).otherwise(1)).cast(IntegerType()))

    df_pandas = data2.toPandas()

    # Read columns from export.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Select necessary columns and export data
    export = data.select([F.col(columna) for columna in nombresColumnasExp]).filter((F.col("Factura") != "FACTURA") & (F.col("Factura") != ""))

    # Convert to Pandas DataFrame
    logging.info("Transformando a Pandas DataFrame")
    pandas_df = export.toPandas()
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Connect to database
    cnn = c.conect(client)

    # Process data insertion
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    logging.info(f"Agregando registros a la base: sim_{client}")
    cursor = cnn.cursor()

    values = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena")).first()["cadena"]
    values2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2")).first()["cadena2"]

    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        logging.info("Registros agregados correctamente")
    except Exception as e:
        cnn.rollback()
        logging.error("Error al cargar los registros: %s", e)

    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    logging.info(f"Finaliza Procesamiento {report}{branch} del Cliente: {client}")
