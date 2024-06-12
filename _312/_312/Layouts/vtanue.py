from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

def clean_column(column, function):
    return function(column).cast(StringType())

def clean_decimal(column):
    return F.when(column.isNotNull(), column).otherwise(0)

def clean_date(column):
    return F.to_date(column, "dd/MM/yyyy").cast(DateType())

def clean_text(column):
    return F.substring(F.col(column).cast(StringType()), 1, 30)

def clean_email(column):
    return F.udf(lambda z: FE.LimpiaEmail(z), StringType())(column)

def clean_data(data):
    transformations = {
        "Client": F.lit(client),
        "Branch": F.lit(branch),
        "Date": F.lit(F.current_date()),
        "Factura": clean_column(F.col("Factura"), FE.LimpiaCodigos),
        "FechaFactura": clean_date(F.col("FechaFactura")),
        "TipoVenta": clean_text("TipoVenta"),
        "TipoPago": clean_text("TipoPago"),
        "Vin": clean_text("Vin"),
        "NumeroInventario": clean_column(F.col("NumeroInventario"), FE.LimpiaCodigos),
        "`Isan$`": clean_decimal(F.col("`Isan$`")),
        "`Costo$`": clean_decimal(F.col("`Costo$`")),
        "`Venta$`": clean_decimal(F.col("`Venta$`")),
        "`Utilidad$`": clean_decimal(F.col("`Venta$`")),
        "Margen": F.when(
            (F.col("`Costo$`") == 0) | (F.col("`Venta$`") == 0), 0
        ).otherwise(
            F.when(F.col("`Venta$`") < 0,
                   ((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * -100
                  ).otherwise(
                   ((F.col("`Venta$`") / F.col("`Costo$`")) - 1) * 100
                  )
        ).cast(DecimalType()),
        "Ano": clean_text("Ano"),
        "Marca": clean_text("Marca"),
        "Modelo": clean_text("Modelo"),
        "Color": clean_text("Color"),
        "Interior": clean_text("Interior"),
        "NumeroVendedor": clean_text("NumeroVendedor"),
        "NombreVendedor": clean_text("NombreVendedor"),
        "FechaCompra": clean_date(F.col("FechaCompra")),
        "FechaEntrega": clean_date(F.col("FechaEntrega")),
        "NombreCliente": clean_text("NombreCliente"),
        "RFC": clean_column(F.col("RFC"), FE.LimpiaCodigos),
        "Direccion": clean_text("Direccion"),
        "Telefono": clean_column(F.col("Telefono"), FE.LimpiaCodigos),
        "CP": clean_text("CP"),
        "Email": clean_email("Email"),
        "VentasNetas": F.when(
            (F.col("`Venta$`") == 0) | (F.col("`Costo$`") == 0), 0
        ).when(
            (F.col("`Venta$`") < 0) | (F.col("`Costo$`") < 0), -1
        ).otherwise(1)
    }
    for column, transformation in transformations.items():
        data = data.withColumn(column, transformation)
    return data

def vtanue01(spark, datasource, columnas, client, branch, report):
    # Load column names
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in imp.collect()]

    # Read and process data
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    data = data.selectExpr([f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)])

    # Clean data
    data = clean_data(data)

    # Filter rows with VIN not equal to "VIN" or empty string
    data = data.filter((F.col("Vin") != "VIN") & (F.col("Vin") != ""))

    # Export data
    expor = spark.read.option("header", True).csv(columnas)
    expor = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"),
                                 F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    # Convert to Pandas DataFrame
    pandas_df = data.toPandas()
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Database operations
    c.cbase(client)
    cnn = c.conect(client)

    # Drop existing data
    s.drop(report, branch, client, cnn)

    # Create database
    s.create(columnas, report, branch, cnn)

    # Insert data
    cursor = cnn.cursor()
    values = expor.first()["cadena"]
    values2 = expor.first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"
   
