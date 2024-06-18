from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, FloatType
from Funciones import FuncionesExternas as FE
from Funciones import conexion as C, Sentencias as s
import pandas as pd
import mysql.connector

def refcom01(spark,datasource,columnas,client,branch,report):
    #Carga funciones definidas
    LCodigos = F.udf(lambda z:FE.LimpiaCodigos(z),StringType())
    LTexto = F.udf(lambda z:FE.LimpiaTexto(z),StringType())


    # Lee el archivo de texto
    imp = spark.read.option("header",True).csv(columnas)
    data = spark.read.text(datasource)
    # Divide cada línea en columnas utilizando el carácter "|"
    data = data.withColumn("columns", F.split(data["value"], "\\|"))
    #lee el nombre de las columnas del archivo import.csv 
    nombresColumnas = [row[1] for row in imp.collect()]
    expresiones = [f"columns[{i}] as {columna}" for i, columna in enumerate(nombresColumnas)]
    #actualiza el nombre de las columnas
    data = data.selectExpr(*expresiones)

    #Columnas runtime
    data = data.withColumn("Client", F.lit(client))
    data = data.withColumn("Branch", F.lit(branch))
    data = data.withColumn("Date",F.lit(F.current_date()))

    #Aplica LimpiaTexto y LimpiaCodigo a las columnas correspondientes
    data = data.withColumn("FechaFactura", (F.to_date(F.col("FechaFactura"),"dd/MM/yy")).cast(DateType()))
    data = data.withColumn("Factura",LCodigos(F.col("Factura")))
    data = data.withColumn("NumeroProveedor",LCodigos(F.col("NumeroProveedor")))
    data = data.withColumn("TipoProveedor",LTexto(F.col("TipoProveedor")))
    data = data.withColumn("NombreProveedor", F.substring( F.col("NombreProveedor").cast("string"),1,30))
    data = data.withColumn("NombreProveedor",LTexto(F.col("NombreProveedor")))
    data = data.withColumn("TipoCompra",LTexto(F.col("TipoCompra")))
    data = data.withColumn("NumeroParte",LTexto(F.col("NumeroParte")))
    data = data.withColumn("Descripcion", F.substring( F.col("Descripcion").cast("string"),1,30))
    data = data.withColumn("Descripcion",LTexto(F.col("Descripcion")))
    data = data.withColumn("`CostoUnit$`", F.col("`CostoUnit$`"))
    data = data.withColumn("Cantidad", F.col("Cantidad"))
    data = data.withColumn("`CostoUnit$`",F.abs(F.col("`CostoUnit$`")))
    data = data.withColumn("`Costo$`",F.col("`Costo$`"))


    # Lee las columnas desde el archivo exportar.csv
    expor = spark.read.option("header", True).csv(columnas)
    nombresColumnasExp = [row[3] for row in expor.collect()]

    # Selecciona las columnas necesarias y exporta los datos
    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    nombresColumnasExp = [row[3] for row in expor.collect()]

    Exp = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col")).alias("cadena"))
    Exp2 = expor.groupBy().agg(F.concat_ws(",", F.collect_list("Col2")).alias("cadena2"))

    

    export = data.select([F.col(columna) for columna in nombresColumnasExp])
    export=export.filter((F.col("Factura") != "FACTURA") & (F.col("Factura") != "") & ((F.col("FechaFactura")).isNotNull()))
    
    ########################################## Se pasa el archivo a DF y se obtienen los meses actualizados ###################
    print("********** Transformado a Pandas DF **********")
    pandas_df = export.toPandas()
    print("ConversionFinalizada")
    tuplas = list(pandas_df.itertuples(index=False,name=None))
    


    #################################### SE LLAMA LA CONEXION CON LA BASE DE DATOS #################################
    C.cbase(client)

    cnn=C.conect(client)

    ################################### COMIENZA PROCESO DE BORRADO DE INFORMACION ##################################
    s.drop(report,branch,client,cnn)

    ################################### COMIENZA PROCESO DE CREACION DE DB ##################################
    s.create(columnas,report,branch,cnn)

    ############################## INSERCIÓN DA DATOS ######################################
    print("********** Agregando registros..." + report + " a la base: " + client + " **********")
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
    s.change(report,branch,cnn,columnas)
    s.export(report,branch,cnn,client)
    
    print("******************************* Finaliza Procesamiento " +report+ "" +branch+ " Del Cliente: " +client+ " ***********************************" )