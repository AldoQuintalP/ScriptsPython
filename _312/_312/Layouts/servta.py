from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, IntegerType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

# Definición de UDFs
LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())
LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

def refser01(spark, datasource, columnas, client, branch, report):
    # Lee el archivo import.csv para obtener los nombres de las columnas
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in imp.collect()]

    # Lee y procesa los datos desde el archivo fuente
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]
    data = data.selectExpr(*expresiones)
    
    # Aplicar transformaciones
    data = (
        data.withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.current_date().cast(DateType()))
            .withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yy").cast(DateType()))
            .withColumn("FechaApertura", F.to_date(F.col("FechaApertura"), "dd/MM/yy").cast(DateType()))
            .withColumn("Dias", F.datediff(F.col("FechaFactura"), F.col("FechaApertura")))
            .withColumn("Factura", LCodigos(F.col("Factura")))
            .withColumn("Taller", F.when(F.col("TipoOrden").rlike("HYP|LYP|CARRO|SEGUR"), "HYP").otherwise("SERVICIO"))
            .withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
            .withColumn("TipoPago", LTexto(F.col("TipoPago")))
            .withColumn("NumeroOT", LCodigos(F.col("NumeroOT")))
            .withColumn("CodigoOperacion", LCodigos(F.col("CodigoOperacion")))
            .withColumn("Descripcion", LTexto(F.col("Descripcion")))
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
            .withColumn("VentasNetas", F.when((F.col("Venta") == 0) | (F.col("Costo") == 0), 0).when((F.col("Venta") < 0) | (F.col("Costo") < 0), -1).otherwise(1))
            .withColumn("Utilidad", F.when((F.col("Costo") == 0) | (F.col("Venta") == 0), 0).otherwise(F.col("Venta") - F.col("Costo")).cast(DecimalType()))
            .withColumn("Margen", F.when((F.col("Costo") == 0) | (F.col("Venta") == 0), 0).otherwise(F.when(F.col("Venta") < 0, ((F.col("Venta") / F.col("Costo")) - 1) * -100).otherwise(((F.col("Venta") / F.col("Costo")) - 1) * 100)).cast(DecimalType()))
            .withColumn("NumeroAsesor", LCodigos(F.col("NumeroAsesor")))
            .withColumn("NombreAsesor", LTexto(F.col("NombreAsesor")))
            .withColumn("RFC", LCodigos(F.col("RFC")))
            .withColumn("NombreCliente", LTexto(F.col("NombreCliente")))
            .withColumn("Direccion", LTexto(F.col("Direccion")))
            .withColumn("Telefono", LCodigos(F.col("Telefono")))
            .withColumn("Telefono", F.when(F.length(F.col("Telefono")) < 2, "").otherwise(F.translate(F.upper(F.col("Telefono")), "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ", "")))
            .withColumn("CP", F.substring(LCodigos(F.col("CP")), 1, 5))
            .withColumn("CP", F.when(F.length(F.col("CP")) < 2, "").otherwise(F.col("CP")))
            .withColumn("Email", LEmail(F.col("Email")))
            .withColumn("Odometro", F.col("Odometro").cast(IntegerType()))
            .withColumn("Vin", LCodigos(F.col("Vin")))
            .withColumn("Ano", F.col("Ano").cast(IntegerType()))
            .withColumn("Marca", LTexto(F.col("Marca")))
            .withColumn("Modelo", LTexto(F.col("Modelo")))
            .withColumn("Color", LTexto(F.col("Color")))
            .withColumn("Interior", LTexto(F.col("Interior")))
            .withColumn("FechaEntrega", F.to_date(F.col("FechaEntrega"), "dd/MM/yy").cast(DateType()))
            .withColumn("TipoOperacion", LTexto(F.col("TipoOperacion")))
    )
    
    # Construcción de SERVTC
    data2 = data.groupBy(
        "FechaFactura", "FechaApertura", "FechaEntrega", "Factura", "Taller", "TipoOrden", "TipoPago", "NumeroOT", 
        "NumeroAsesor", "NombreAsesor", "RFC", "NombreCliente", "Direccion", "Telefono", "CP", "Email", 
        "Odometro", "Vin", "Ano", "Marca", "Modelo", "Color", "Dias"
    ).agg(
        F.sum(F.col("Venta")).alias("Venta"), F.sum(F.col("Costo")).alias("Costo"), F.sum(F.col("Utilidad")).alias("Utilidad"),
        F.sum(F.col("Margen")).alias("Margen"), F.sum(F.col("VentaMO")).alias("VentaMO"), F.sum(F.col("DescuentoMO")).alias("DescuentoMO"),
        F.sum(F.col("CostoMO")).alias("CostoMO"), F.sum(F.col("VentaMateriales")).alias("VentaMateriales"), F.sum(F.col("DescuentoMateriales")).alias("DescuentoMateriales"),
        F.sum(F.col("CostoMateriales")).alias("CostoMateriales"), F.sum(F.col("VentaTOT")).alias("VentaTOT"), F.sum(F.col("DescuentoTOT")).alias("DescuentoTOT"),
        F.sum(F.col("CostoTOT")).alias("CostoTOT"), F.sum(F.col("VentaPartes")).alias("VentaPartes"), F.sum(F.col("DescuentoPartes")).alias("DescuentoPartes"),
        F.sum(F.col("CostoPartes")).alias("CostoPartes"), F.sum(F.col("VentaTotal")).alias("VentaTotal"), F.sum(F.col("CostoTotal")).alias("CostoTotal")
    ).withColumn("Client", F.lit(client)).withColumn("Branch", F.lit(branch)).withColumn("VentasNetas", F.when(F.col("Venta") == 0, 0).otherwise(F.when(F.col("Venta") < 0, -1).otherwise(1))).withColumn("VentasNetas", F.col("VentasNetas").cast(IntegerType()))

    # Conversión a Pandas y preparación para la base de datos
    df_pandas = data2.toPandas()
    tuplas = list(df_pandas.itertuples(index=False, name=None))

    # Conexión a la base de datos y manipulación de datos
    c.cbase(client)
    cnn = c.conect(client)
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)
    cursor = cnn.cursor()

    # Lee las columnas desde el archivo exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    values = Exp.first()["cadena"]
    values2 = Exp2.first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        print("********** Registros agregados correctamente **********")
    except:
        cnn.rollback()
        print("********** Error al cargar los registros **********")

    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    print(f"******************************* Finaliza Procesamiento {report}{branch} Del Cliente: {client} ***********************************")
