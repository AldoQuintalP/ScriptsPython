from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, IntegerType
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
    data = data.withColumn("FechaFactura", (F.to_date(F.col("FechaFactura"),"dd/MM/yy")).cast(DateType()))
    data = data.withColumn("FechaApertura", (F.to_date(F.col("FechaApertura"),"dd/MM/yy")).cast(DateType()))
    data = data.withColumn("Dias",F.datediff(F.col("FechaFactura"),F.col("FechaApertura")))
    data = data.withColumn("Factura",LCodigos(F.col("Factura")))
    data = data.withColumn("Taller",
                           F.when((F.instr(F.col("TipoOrden"),"HYP") != 0) |
                                   (F.instr(F.col("TipoOrden"),"LYP") != 0) |
                                   (F.instr(F.col("TipoOrden"),"CARRO") != 0) |
                                   (F.instr(F.col("TipoOrden"),"SEGUR") != 0),"HYP")
                           .otherwise("SERVICIO"))    
    data = data.withColumn("TipoOrden",LTexto(F.col("TipoOrden")))
    data = data.withColumn("TipoPago",LTexto(F.col("TipoPago")))
    data = data.withColumn("NumeroOT",LCodigos(F.col("NumeroOT")))
    data = data.withColumn("CodigoOperacion",LCodigos(F.col("CodigoOperacion")))
    data = data.withColumn("Descripcion",LTexto(F.col("Descripcion")))
    data = data.withColumn("VentaMO", F.col("VentaMO").cast(DecimalType()))
    data = data.withColumn("DescuentoMO", F.col("DescuentoMO").cast(DecimalType()))
    data = data.withColumn("CostoMO", F.col("CostoMO").cast(DecimalType()))
    data = data.withColumn("VentaMateriales",F.col("VentaMateriales").cast(DecimalType()))
    data = data.withColumn("DescuentoMateriales",F.col("DescuentoMateriales").cast(DecimalType()))
    data = data.withColumn("CostoMateriales",F.col("CostoMateriales").cast(DecimalType()))
    data = data.withColumn("VentaTOT",F.col("VentaTOT").cast(DecimalType()))
    data = data.withColumn("DescuentoTOT",F.col("DescuentoTOT").cast(DecimalType()))
    data = data.withColumn("CostoTOT",F.col("CostoTOT").cast(DecimalType()))
    data = data.withColumn("VentaPartes",F.col("VentaPartes").cast(DecimalType()))
    data = data.withColumn("DescuentoPartes",F.col("DescuentoPartes").cast(DecimalType()))
    data = data.withColumn("CostoPartes",F.col("CostoPartes").cast(DecimalType()))
    data = data.withColumn("Venta", (F.col("VentaMO") + F.col("VentaMateriales") + F.col("VentaTOT")).cast(DecimalType()))
    data = data.withColumn("Costo", (F.col("CostoMO") + F.col("CostoMateriales") + F.col("CostoTOT")).cast(DecimalType()))
    data = data.withColumn("VentaTotal",F.col("VentaTotal").cast(DecimalType()))
    data = data.withColumn("CostoTotal",F.col("CostoTotal").cast(DecimalType()))
    data = data.withColumn("VentasNetas", 
                      F.when((F.col("`Venta$`") == 0) | (F.col("`Costo$`") == 0), 0)
                      .when((F.col("`Venta$`") < 0) | (F.col("`Costo$`") < 0), -1)
                      .otherwise(1)
                      )
    data = data.withColumn("Utilidad", 
                           F.when((F.col("Costo")==0) | (F.col("Venta")==0), 0).otherwise(F.col("Venta") - F.col("Costo")))
    data = data.withColumn("Utilidad", F.col("Utilidad").cast(DecimalType()))
    data = data.withColumn("Margen", 
                           F.when((F.col("Costo")==0) | (F.col("Venta")==0), 0)
                           .otherwise(F.when(F.col("Venta")<0,((F.col("Venta")/F.col("Costo"))-1)*-100).otherwise(((F.col("Venta")/F.col("Costo"))-1)*100)))
    data = data.withColumn("Margen", F.col("Margen").cast(DecimalType()))
    data = data.withColumn("NumeroAsesor",LCodigos(F.col("NumeroAsesor")))
    data = data.withColumn("NombreAsesor",LTexto(F.col("NombreAsesor")))
    data = data.withColumn("RFC",LCodigos(F.col("RFC")))
    data = data.withColumn("NombreCliente",LTexto(F.col("NombreCliente")))
    data = data.withColumn("Direccion",LTexto(F.col("Direccion")))
    data = data.withColumn("Telefono",LCodigos("Telefono"))
    data = data.withColumn("Telefono",
                           F.when((F.length(F.col("Telefono")) < 2),"")
                           .otherwise(F.translate(F.upper(F.when((F.instr(F.col("Telefono"),",") != 0),F.split(F.col("Telefono"),",",2).getItem(1).alias("Telefono"))
                                                          .otherwise(F.when((F.instr(F.col("Telefono"),";") != 0),F.split(F.col("Telefono"),";",2).getItem(1).alias("Telefono"))
                                                                     .otherwise(F.when((F.instr(F.col("Telefono"),"/") != 0),F.split(F.col("Telefono"),"/",2).getItem(1).alias("Telefono"))
                                                                                .otherwise(F.when((F.instr(F.col("Telefono"),"Y") != 0),F.split(F.col("Telefono"),"Y",2).getItem(1).alias("Telefono"))
                                                                                           .otherwise(F.when((F.instr(F.col("Telefono"),"EXT") != 0),F.split(F.col("Telefono"),"EXT",2).getItem(1).alias("Telefono")).otherwise(F.col("Telefono"))))))),"ABCDEFGHIJKLMNÑOPQRSTUVWXYZ","")))
    data = data.withColumn("CP",F.substring(LCodigos(F.col("CP")),1,5).alias("CP"))
    data = data.withColumn("CP",F.when(F.length(F.col("CP")) < 2,"").otherwise(F.col("CP")))
    data = data.withColumn("Email",LEmail(F.col("Email")))
    data = data.withColumn("Odometro",F.col("Odometro").cast(IntegerType()))
    data = data.withColumn("Vin",LCodigos(F.col("Vin")))
    data = data.withColumn("Ano",F.col("Ano").cast(IntegerType()))
    data = data.withColumn("Marca",LTexto(F.col("Marca")))
    data = data.withColumn("Modelo",LTexto(F.col("Modelo")))
    data = data.withColumn("Color",LTexto(F.col("Color")))
    data = data.withColumn("Interior",LTexto(F.col("Interior")))
    data = data.withColumn("FechaEntrega", (F.to_date(F.col("FechaEntrega"),"dd/MM/yy")).cast(DateType()))
    data = data.withColumn("TipoOperacion",LTexto(F.col("TipoOperacion")))

    #Finaliza construcción de SERVTA


    #servtc
    print("Construyendo SERVTC")

    data2 = export.groupBy("FechaFactura","FechaApertura","FechaEntrega","FechaCierre","Factura","Taller","TipoOrden","TipoPago","NumeroOT","NumeroAsesor","NombreAsesor","RFC","NombreCliente","Direccion","Telefono","CP","Email","Odometro","Vin","Ano","Marca","Modelo","Color","Dias").agg(
      F.sum(F.col("Venta")).alias("Venta"),F.sum(F.col("Costo")).alias("Costo"),F.sum(F.col("Utilidad")).alias("Utilidad"),F.sum(F.col("Margen")).alias("Margen"),F.sum(F.col("VentaMO")).alias("VentaMO"),F.sum(F.col("DescuentoMO")).alias("DescuentoMO"),F.sum(F.col("CostoMO")).alias("CostoMO"),F.sum(F.col("VentaMateriales")).alias("VentaMateriales"),F.sum(F.col("DescuentoMateriales")).alias("DescuentoMateriales"),F.sum(F.col("CostoMateriales")).alias("CostoMateriales"),F.sum(F.col("VentaTOT")).alias("VentaTOT"),F.sum(F.col("DescuentoTOT")).alias("DescuentoTOT"),F.sum(F.col("CostoTOT")).alias("CostoTOT"),F.sum(F.col("VentaPartes")).alias("VentaPartes"),F.sum(F.col("DescuentoPartes")).alias("DescuentoPartes"),F.sum(F.col("CostoPartes")).alias("CostoPartes"),F.sum(F.col("VentaTotal")).alias("VentaTotal"),F.sum(F.col("CostoTotal")).alias("CostoTotal"))

    data2 = data2.withColumn("Client", F.lit(client))
    data2 = data2.withColumn("Branch", F.lit(branch))
    data2 = data2.withColumn("VentasNetas",
                            F.when(F.col("Venta")==0, 0)
                            .otherwise(F.when(F.col("Venta")<0, -1).otherwise(1)))
    data2 = data2.withColumn("VentasNetas", F.col("VentasNetas").cast(IntegerType()))

    df_pandas=data2.toPandas()
    
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