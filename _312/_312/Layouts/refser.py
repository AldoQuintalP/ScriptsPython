from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from Funciones import FuncionesExternas as FE, Sentencias as s, conexion as c

def refser01(spark, datasource, columnas, client, branch, report):
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
    
    #data.show()
    # Realiza las transformaciones en cadena
    data = data.withColumn("Client", F.lit(client))
    data = data.withColumn("Branch", F.lit(branch))
    data = data.withColumn("Date", F.lit(F.current_date()))
    data = data.withColumn("Factura", LTexto(F.col("Factura")))
    data = data.withColumn("FechaFactura", F.to_date(F.col("FechaFactura"), "dd/MM/yyyy").cast(DateType()))
    data = data.withColumn("NumeroParte", LTexto(F.col("NumeroParte")))
    data = data.withColumn("Descripcion", F.substring( F.col("Descripcion").cast("string"),1,30))
    data = data.withColumn("Descripcion",LTexto(F.col("Descripcion")))
    data = data.withColumn("Cantidad", F.col("Cantidad"))
    data = data.withColumn("`VentaUnit$`", F.when(F.col("`VentaUnit$`").isNotNull(), F.col("`VentaUnit$`")).otherwise(0).cast(DecimalType(18,4)))
    data = data.withColumn("`Venta$`", F.when(F.col("`Venta$`").isNotNull(), F.col("`Venta$`")).otherwise(0).cast(DecimalType(35,10)))
    data = data.withColumn("`CostoUnit$`", F.when(F.col("`CostoUnit$`").isNotNull(), F.col("`CostoUnit$`")).otherwise(0).cast(DecimalType(18,4)))
    data = data.withColumn("`Costo$`", F.when(F.col("`Costo$`").isNotNull(), F.col("`Costo$`")).otherwise(0).cast(DecimalType(35,10)))
    data = data.withColumn("`Utilidad$`", F.when(F.col("`Utilidad$`").isNotNull(), F.col("`Venta$`")).otherwise(0).cast(DecimalType(18,4)))
    data = data.withColumn("Margen", F.col("Margen").cast(DecimalType()))
    data = data.withColumn("Margen", 
                           F.when((F.col("`Costo$`")==0) | (F.col("`Venta$`")==0), 0)
                           .otherwise(F.when(F.col("`Venta$`")<0,((F.col("`Venta$`")/F.col("`Costo$`"))-1)*-100).otherwise(((F.col("`Venta$`")/F.col("`Costo$`"))-1)*100)))
    data = data.withColumn("RFC", F.substring(F.col("RFC").cast("string"),1,13))
    data = data.withColumn("RFC", LCodigos(F.col("RFC")))
    data = data.withColumn("NumeroOT", LTexto(F.col("NumeroOT")))
    data = data.withColumn("TipoOrden", LTexto(F.col("TipoOrden")))
    data = data.withColumn("Taller", LTexto(F.col("Taller")))

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
    export=export.filter((F.col("Factura") != "FACTURA") & (F.col("Factura") != ""))
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