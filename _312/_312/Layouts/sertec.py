from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

def sertec01(spark, datasource, columnas, client, branch, report):
    # Load defined functions
    LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())
    LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

    # Read the import.csv file to get the column names
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
            .withColumn("NumeroOT", F.substring(F.col("NumeroOT").cast("string"), 1, 10))
            .withColumn("NumeroOT", LTexto(F.col("NumeroOT")))
            .withColumn("CodigoOperacion", LTexto(F.col("CodigoOperacion")))
            .withColumn("FechaCierre", F.to_date(F.col("FechaCierre"), "dd/MM/yyyy").cast(DateType()))
            .withColumn("Taller", LTexto(F.col("Taller")))
            .withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
            .withColumn("HorasPagadas", F.col("HorasPagadas").cast(DecimalType(18, 2)))
            .withColumn("HorasFacturadas", F.col("HorasFacturadas").cast(DecimalType(18, 2)))
            .withColumn("NumeroMecanico", LTexto(F.col("NumeroMecanico")))
            .withColumn("NombreMecanico", LTexto(F.col("NombreMecanico")))
           )

    # Read export.csv to get export column names
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Select necessary columns and filter data
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export = export.filter((F.col("NumeroOT") != "NUMEROOT") & (F.col("NumeroOT") != ""))

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
    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    values = Exp.first()["cadena"]
    values2 = Exp2.first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        print("********** Records added successfully **********")
    except:
        cnn.rollback()
        print("********** Error adding records **********")

    # Apply changes and export data
    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    print(f"******************************* Processing complete for {report}{branch} for client {client} ***********************************")
