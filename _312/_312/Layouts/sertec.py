from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

def sertec01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())
    LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

    # Lee el archivo import.csv para obtener los nombres de las columnasa
    imp = spark.read.option("header", True).csv(columnas)
    nombresColumnas = [row[1] for row in imp.collect()]

    # Lee y procesa los datos desde el archivo fuente
    data = spark.read.text(datasource)
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]
    data = data.selectExpr(*expresiones)

    # Realiza las transformaciones en cadena
    data = data.withColumn("Client", F.lit(client))
    data = data.withColumn("Branch", F.lit(branch))
    data = data.withColumn("Date", F.lit(F.current_date()))
    data = data.withColumn("NumeroOT", F.substring( F.col("NumeroOT").cast("string"),1,10))
    data = data.withColumn("NumeroOT", LTexto(F.col("NumeroOT")))
    data = data.withColumn("CodigoOperacion", LTexto(F.col("CodigoOperacion")))
    data = data.withColumn("FechaCierre", F.to_date(F.col("FechaCierre"), "dd/MM/yyyy").cast(DateType()))
    data = data.withColumn("Taller", LTexto(F.col("Taller")))
    data = data.withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
    data = data.withColumn("HorasPagadas",  F.col("HorasPagadas").cast(DecimalType(18,2)))
    data = data.withColumn("HorasFacturadas",  F.col("HorasFacturadas").cast(DecimalType(18,2)))
    data = data.withColumn("NumeroMecanico", LTexto(F.col("NumeroMecanico")))
    data = data.withColumn("NombreMecanico", LTexto(F.col("NombreMecanico")))


    #data.show()
    # Lee las columnas desde el archivo exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    nombresColumnasExp = [row[3] for row in expor.collect()]

    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))
    
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export=export.filter((F.col("NumeroOT") != "NUMEROOT") & (F.col("NumeroOT") != ""))
    #export.show()
    ########################################## Se pasa el archivo a DF y se obtienen los meses actualizados ###################
    print("********** Transformado a Pandas DF **********")
    pandas_df = export.toPandas()
    print("ConversionFinalizada")
    tuplas = list(pandas_df.itertuples(index=False,name=None))
    


    #################################### SE LLAMA LA CONEXION CON LA BASE DE DATOS #################################
    c.cbase(client)

    cnn=c.conect(client)

    ################################### COMIENZA PROCESO DE BORRADO DE INFORMACION ##################################
    s.drop(report,branch,client,cnn)

    ################################### COMIENZA PROCESO DE CREACION DE DB ##################################
    s.create(columnas,report,branch,cnn)

    ############################## INSERCIÓN DA DATOS ######################################
    print("********** Agregando registros..." + report + "" +branch+ " a la base: sim_" + client + " **********")
    cursor = cnn.cursor()

    values = Exp.first()["cadena"]
    values2 = Exp2.first()["cadena2"]
    #(Client,Branch,Date,NumeroOT,FechaEntrega,FechaCierre,Taller,TipoOrden,TipoServicio,Motivo,Status,NumeroParte,Descripcion,Cantidad,VentaUnit,Venta,CostoUnit,Costo,Utilidad,Margen,Vin,RFC,Modelo,Version)
    sql = f"INSERT INTO {report}{branch} ({values}) VALUES({values2})"

    try:
        cursor.executemany(sql,tuplas)
        cnn.commit()
        print("********** Registros agregados correctamente **********")
    
    except:
        cnn.rollback()
        print("********** Error al cargar los registros **********")
                #cnn.close()
    ############################## INSERCIÓN DA DATOS ######################################
    s.change(report, branch, cnn,columnas)
    s.export(report,branch,cnn,client)
    print("******************************* Finaliza Procesamiento " +report+ "" +branch+ " Del Cliente: " +client+ " ***********************************" )