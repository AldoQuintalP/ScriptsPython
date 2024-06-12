from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sertec01(spark, datasource, columnas, client, branch, report):
    logging.info("Inicio del procesamiento de sertec01")
    # Define UDFs
    LCodigos = F.udf(FE.LimpiaCodigos, StringType())
    LTexto = F.udf(FE.LimpiaTexto, StringType())
    LEmail = F.udf(FE.LimpiaEmail, StringType())

    # Read import.csv to get column names
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in imp.collect()]

    # Read and process data from the source file
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(F.col("value"), "\\|"))
    data = data.select([F.col(f"columns[{i}]").alias(nombresColumnas[i]) for i in range(len(nombresColumnas))])

    # Perform transformations
    data = (
        data.withColumn("Client", F.lit(client))
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

    # Read export columns from exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Select necessary columns and filter the data
    export = data.select([F.col(columna) for columna in nombresColumnasExp]).filter((F.col("NumeroOT") != "NUMEROOT") & (F.col("NumeroOT") != ""))

    # Convert to Pandas DataFrame
    print("********** Transformado a Pandas DF **********")
    pandas_df = export.toPandas()
    print("Conversion Finalizada")
    tuplas = list(pandas_df.itertuples(index=False, name=None))

    # Database connection and operations
    cnn = c.conect(client)
    s.drop(report, branch, client, cnn)
    s.create(columnas, report, branch, cnn)

    # Insert data into the database
    print(f"********** Agregando registros... {report}{branch} a la base: sim_{client} **********")
    cursor = cnn.cursor()

    values = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena")).first()["cadena"]
    values2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2")).first()["cadena2"]
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql, tuplas)
        cnn.commit()
        print("********** Registros agregados correctamente **********")
    except Exception as e:
        cnn.rollback()
        print(f"********** Error al cargar los registros: {e} **********")

    s.change(report, branch, cnn, columnas)
    s.export(report, branch, cnn, client)
    print(f"******************************* Finaliza Procesamiento {report}{branch} Del Cliente: {client} ***********************************")
