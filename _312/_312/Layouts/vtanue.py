from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def vtanue01(spark, datasource, columnas, client, branch, report):
    logging.info("Inicio del procesamiento de vtanue01")
    # Carga funciones definidas
    LCodigos = F.udf(FE.LimpiaCodigos, StringType())
    LTexto = F.udf(FE.LimpiaTexto, StringType())
    LEmail = F.udf(FE.LimpiaEmail, StringType())

    # Lee el archivo import.csv para obtener los nombres de las columnas
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row["name"] for row in imp.collect()]

    # Lee y procesa los datos desde el archivo fuente
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]
    data = data.selectExpr(*expresiones)

    # Realiza las transformaciones en cadena
    data = (
        data.withColumn("Client", F.lit(client))
            .withColumn("Branch", F.lit(branch))
            .withColumn("Date", F.current_date())
            .withColumn("Factura", LCodigos(F.col("Factura")))
            .withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yyyy"))
            .withColumn("TipoVenta", LTexto(F.col("TipoVenta")))
            .withColumn("TipoPago", LTexto(F.col("TipoPago")))
            .withColumn("Vin", LTexto(F.substring(F.col("Vin"), 1, 20)))
            .withColumn("NumeroInventario", LCodigos(F.col("NumeroInventario")))
            .withColumn("`Isan$`", F.when(F.col("`Isan$`").isNotNull(), F.col("`Isan$`")).otherwise(0).cast(DecimalType(18, 4)))
            .withColumn("`Costo$`", F.when(F.col("`Costo$`").isNotNull(), F.col("`Costo$`")).otherwise(0).cast(DecimalType(35, 10)))
            .withColumn("`Venta$`", F.when(F.col("`Venta$`").isNotNull(), F.col("`Venta$`")).otherwise(0).cast(DecimalType(35, 10)))
            .withColumn("`Utilidad$`", F.when(F.col("`Utilidad$`").isNotNull(), F.col("`Utilidad$`")).otherwise(0).cast(DecimalType(18, 4)))
            .withColumn("Margen", F.when((F.col("`Costo$`") == 0) | (F.col("`Venta$`") == 0), 0)
                         .otherwise(F.when(F.col("`Venta$`") < 0, ((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * -100)
                         .otherwise(((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * 100)).cast(DecimalType()))
            .withColumn("Ano", LTexto(F.col("Ano")))
            .withColumn("Marca", LTexto(F.col("Marca")))
            .withColumn("Modelo", LTexto(F.substring(F.col("Modelo"), 1, 30)))
            .withColumn("Color", LTexto(F.col("Color")))
            .withColumn("Interior", LTexto(F.col("Interior")))
            .withColumn("NumeroVendedor", LTexto(F.col("NumeroVendedor")))
            .withColumn("NombreVendedor", LTexto(F.substring(F.col("NombreVendedor"), 1, 30)))
            .withColumn("FechaCompra", F.to_date(F.col("FechaCompra"), "dd/MM/yyyy"))
            .withColumn("FechaEntrega", F.to_date(F.col("FechaEntrega"), "dd/MM/yyyy"))
            .withColumn("NombreCliente", LTexto(F.substring(F.col("NombreCliente"), 1, 30)))
            .withColumn("RFC", LCodigos(F.substring(F.col("RFC"), 1, 13)))
            .withColumn("Direccion", LTexto(F.col("Direccion")))
            .withColumn("Telefono", LCodigos(F.col("Telefono")))
            .withColumn("CP", LTexto(F.col("CP")))
            .withColumn("Email", LEmail(F.col("Email")))
            .withColumn("VentasNetas", F.when((F.col("`Venta$`") == 0) | (F.col("`Costo$`") == 0), 0)
                      .when((F.col("`Venta$`") < 0) | (F.col("`Costo$`") < 0), -1)
                      .otherwise(1))
    )

    # Lee las columnas desde el archivo exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row["name"] for row in expor.collect()]

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("Vin") != "VIN") & (F.col("Vin") != ""))

    # Transformar a Pandas DF y obtener tuplas
    logging.info("Transformando a Pandas DataFrame")
    pandas_df = export.toPandas()
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Conexión con la base de datos
    c.cbase(client)
    cnn = c.conect(client)

    # Proceso de borrado y creación de DB
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    # Inserción de datos
    logging.info(f"Agregando registros a la base: sim_{client}")
    cursor = cnn.cursor()
    values = ",".join(nombresColumnasExp)
    placeholders = ",".join(["%s"] * len(nombresColumnasExp))
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES ({placeholders})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        logging.info("Registros agregados correctamente")
    except Exception as e:
        cnn.rollback()
        logging.error("Error al cargar los registros: %s", e)

    # Cambios y exportación
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    logging.info(f"Finaliza Procesamiento {report}{branch} del Cliente: {client}")
