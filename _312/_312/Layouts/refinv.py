from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, DoubleType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

def refinv01(spark, datasource, columnas, client, branch, report):
    # Carga funciones definidas
    LCodigos = F.udf(lambda z: FE.LimpiaCodigos(z), StringType())
    LTexto = F.udf(lambda z: FE.LimpiaTexto(z), StringType())
    LEmail = F.udf(lambda z: FE.LimpiaEmail(z), StringType())

    # Lee el archivo import.csv para obtener los nombres de las columnas
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
    
    data = data.withColumn("NumeroParte", LTexto(F.col("NumeroParte")))
    data = data.withColumn("Descripcion", F.substring( F.col("Descripcion").cast("string"),1,30))
    data = data.withColumn("Descripcion", LTexto(F.col("Descripcion")))
    data = data.withColumn("TipoParte", LTexto(F.col("TipoParte")))
    data = data.withColumn("Almacen", F.substring( F.col("Almacen").cast("string"),1,10))
    data = data.withColumn("Almacen", LTexto(F.col("Almacen")))
    data = data.withColumn("Existencia", F.col("Existencia").cast(DecimalType(15,4)))
    data = data.withColumn("`CostoUnit$`",  F.col("`CostoUnit$`").cast(DecimalType(18,4)))
    data = data.withColumn("`Costo$`", F.col("`Costo$`").cast(DecimalType(35,10)))
    data = data.withColumn("`Precio$`", F.col("`Precio$`").cast(DecimalType(18,4)))
    data = data.withColumn("`Precio2$`", F.col("`Precio2$`").cast(DecimalType(18,4)))
    data = data.withColumn("`Precio3$`", F.col("`Precio3$`").cast(DecimalType(18,4)))
    data = data.withColumn("`Precio4$`",LTexto(F.col("`Precio4$`")))
    data = data.withColumn(
                        "`Precio4$`",
                        F.when(F.isnull(F.col("`Precio4$`")) | (F.col("`Precio4$`") == ""), F.lit(0).cast(DoubleType()))
                        .otherwise(F.col("`Precio4$`").cast(DoubleType()))
                                )
    #data = data.withColumn("`Precio4$`", F.when(F.col("`Precio4$`").isNull() | (F.col("`Precio4$`") == ''), 0).otherwise(F.col("`Precio4$`")))
    #data = data.withColumn("`Precio4$`", F.col("`Precio4$`").cast(DecimalType(18,4)))
    #data = data.withColumn("`Precio4$`", F.when(F.col("`Precio4$`").isNotNull(), F.col("`Precio4$`")).otherwise(0).cast(DecimalType(18,4)))
    #sdata = data.withColumn("`Precio5$`", F.col("`Precio5$`").cast(DecimalType(18,4)))
    data = data.withColumn("UltimaCompra", F.to_date(F.col("UltimaCompra"), "dd/MM/yyyy").cast(DateType()))
    data = data.withColumn("UltimaVenta", F.to_date(F.col("UltimaVenta"), "dd/MM/yyyy").cast(DateType()))
    data = data.withColumn("FechaAlta", F.to_date(F.col("FechaAlta"), "dd/MM/yyyy").cast(DateType()))
    #data = data.withColumn("Clasificacion", LTexto(F.col("Clasificacion")))

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
    export=export.filter((F.col("NumeroParte") != "NUMERO") & (F.col("NumeroParte") != ""))
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