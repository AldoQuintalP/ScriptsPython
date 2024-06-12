import xlsxwriter
from Funciones import paths as p
from Funciones import Descompress as s
import csv






def invnue():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\invnue01.csv'

    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'Blank',            1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Vin',              2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'NumeroInventario', 3,  'Date',             '%s',   'date'],
                [5,     'Ano',              4,  'Vin',              '%s',   'varchar(17)'],
                [6,     'Marca',            5,  'NumeroInventario', '%s',   'varchar(10)'],
                [7,     'Modelo',           6,  'Ano',              '%s',   'int(11)'],
                [8,     'Version',          7,  'Marca',            '%s',   'varchar(10)'],
                [9,     'Color',            8,  'Modelo',           '%s',   'varchar(30)'],
                [10,    'Interior',         9,  'Version',          '%s',   'varchar(15)'],
                [11,    '`Costo$`',         10, 'Color',            '%s',   'varchar(30)'],
                [12,    'FechaCompra',      11, 'Interior',         '%s',   'varchar(15)'],
                [13,    'Status',           12, '`Costo$`',         '%s',   'decimal(35,10)'], 
                [14,    'TipoCompra',       13, 'FechaCompra',      '%s',   'date'],
                ['',    '',                 14, 'Dias',             '%s',   'int(11)'], 
                ['',    '',                 15, 'Status',           '%s',   'varchar(10)'],                            
                ['',    '',                 16, 'TipoCompra',       '%s',   'varchar(30)']]
        
        for m in model:
            writer.writerow(m)

def invusa():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\invusa01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'Blank',            1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Vin',              2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'NumeroInventario', 3,  'Date',             '%s',   'date'],
                [5,     'Ano',              4,  'Vin',              '%s',   'varchar(17)'],
                [6,     'Marca',            5,  'NumeroInventario', '%s',   'varchar(10)'],
                [7,     'Modelo',           6,  'Ano',              '%s',   'int(11)'],
                [8,     'Version',          7,  'Marca',            '%s',   'varchar(10)'],
                [9,     'Color',            8,  'Modelo',           '%s',   'varchar(30)'],
                [10,    'Interior',         9,  'Version',          '%s',   'varchar(15)'],
                [11,    '`Costo$`',         10, 'Color',            '%s',   'varchar(30)'],
                [12,    '`Isan$`',          11, 'Interior',         '%s',   'varchar(15)'],
                [13,    '`CostoCompra$`',   12, '`Costo$`',         '%s',   'decimal(35,10)'], 
                [14,    'FechaCompra',      13, 'FechaCompra',      '%s',   'date'],
                [15,    'Status',           14, 'Dias',             '%s',   'int(11)'], 
                [16,    'TipoCompra',       15, 'Status',           '%s',   'varchar(10)'],                            
                ['',    '',                 16, 'TipoCompra',       '%s',   'varchar(30)']]
        
        for m in model:
            writer.writerow(m)

def refcom():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\refcom01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                   1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Factura',              2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaFactura',         3,  'Factura',          '%s',   'varchar(15)'],
                [5,     'NumeroParte',          4,  'FechaFactura',     '%s',   'date'],
                [6,     'Descripcion',          5,  'TipoProveedor',    '%s',   'varchar(30)'],
                [7,     'Cantidad',             6,  'NumeroProveedor',  '%s',   'varchar(10)'],
                [8,     '`CostoUnit$`',         7,  'NombreProveedor',  '%s',   'varchar(30)'],
                [9,     '`Costo$`',             8,  'TipoCompra',       '%s',   'varchar(30)'],
                [10,    'TipoProveedor',        9,  'NumeroParte',      '%s',   'varchar(20)'],
                [11,    'TipoCompra',           10, 'Descripcion',      '%s',   'varchar(30)'],
                [12,    'NumeroProveedor',      11, 'Cantidad',         '%s',   'decimal(15,4)'],
                [13,    'NombreProveedor',      12, '`CostoUnit$`',     '%s',   'decimal(18,4)'], 
                ['',    '',                     13, '`Costo$`',         '%s',   'decimal(35,10)'],
                ]
        
        for m in model:
            writer.writerow(m)


def refinv():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\refinv01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                   1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'NumeroParte',          2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'Descripcion',          3,  'Date',             '%s',   'date'],
                [5,     'Almacen',              4,  'NumeroParte',      '%s',   'varchar(20)'],
                [6,     'Existencia',           5,  'Descripcion',      '%s',   'varchar(30)'],
                [7,     '`CostoUnit$`',         6,  'Almacen',          '%s',   'varchar(10)'],
                [8,     '`Costo$`',             7,  'Existencia',       '%s',   'decimal(15,4)'],
                [9,     '`Precio$`',            8,  '`CostoUnit$`',     '%s',   'decimal(18,4)'],
                [10,    '`Precio2$`',           9,  '`Costo$`',         '%s',   'decimal(35,10)'],
                [11,    '`Precio3$`',           10, '`Precio$`',        '%s',   'decimal(18,4)'],
                [12,    '`Precio4$`',           11, '`Precio2$`',       '%s',   'decimal(18,4)'],
                [13,    '`Precio5$`',           12, '`Precio3$`',       '%s',   'decimal(18,4)'], 
                [14,    'UltimaCompra',         13, '`Precio4$`',       '%s',   'varchar(10)'],
                [15,    'UltimaVenta',          14, '`Precio5$`',       '%s',   'decimal(18,4)'],
                [16,    'FechaAlta',            15, 'UltimaCompra',     '%s',   'date'],
                [17,    'TipoParte',            16, 'UltimaVenta',      '%s',   'date'],
                [18,    'Clasificacion',        17, 'FechaAlta',        '%s',   'date'],
                ['',    '',                     18, 'TipoParte',        '%s',   'varchar(30)'],

                ]
        
        for m in model:
            writer.writerow(m)

def refmos():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\refmos01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                   1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Factura',              2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaFactura',         3,  'Factura',          '%s',   'varchar(15)'],
                [5,     'NumeroParte',          4,  'FechaFactura',     '%s',   'date'],
                [6,     'Cantidad',             5,  'TipoPago',         '%s',   'varchar(30)'],
                [7,     '`CostoUnit$`',         6,  'TipoVenta',        '%s',   'varchar(30)'],
                [8,     '`Costo$`',             7,  'TipoParte',        '%s',   'varchar(30)'],
                [9,     '`VentaUnit$`',         8,  'NumeroParte',      '%s',   'varchar(20)'],
                [10,    '`Venta$`',             9,  'Cantidad',         '%s',   'decimal(15,4)'],
                [11,    '`Utilidad$`',          10, '`VentaUnit$`',     '%s',   'decimal(18,4)'],
                [12,    'Margen',               11, '`Venta$`',         '%s',   'decimal(35,10)'],
                [13,    'NumeroVendedor',       12, '`CostoUnit$`',     '%s',   'decimal(18,4)'], 
                [14,    'NombreVendedor',       13, '`Costo$`',         '%s',   'decimal(35,10)'],
                [15,    'NombreCliente',        14, '`Utilidad$`',      '%s',   'decimal(18,4)'],
                [16,    'RFC',                  15, 'Margen',           '%s',   'decimal(15,4)'],
                [17,    'Direccion',            16, 'NumeroVendedor',   '%s',   'varchar(10)'],
                [18,    'Telefono',             17, 'NombreVendedor',   '%s',   'varchar(30)'],
                [19,    'CP',                   18, 'RFC',              '%s',   'varchar(13)'],
                [20,    'Email',                19, 'NombreCliente',    '%s',   'varchar(30)'],
                [21,    'TipoVenta',            20, 'Direccion',        '%s',   'varchar(80)'],
                [22,    'TipoPago',             21, 'Telefono',         '%s',   'varchar(15)'],
                [23,    'TipoParte',            22, 'CP',               '%s',   'char(5)'],
                ['',    '',                     23, 'Email',            '%s',   'varchar(40)'],
                ['',    '',                     24, 'VentasNetas',      '%s',   'tinyint(4)'],

                ]
        
        for m in model:
            writer.writerow(m)

def refoep():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\refoep01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                   1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'NumeroOT',             2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaEntrega',         3,  'Date',             '%s',   'date'],
                [5,     'NumeroParte',          4,  'NumeroOT',         '%s',   'varchar(10)'],
                [6,     'Descripcion',          5,  'Taller',           '%s',   'varchar(30)'],
                [7,     'Cantidad',             6,  'TipoOrden',        '%s',   'varchar(30)'],
                [8,     '`CostoUnit$`',         7,  'FechaEntrega',     '%s',   'date'],
                [9,     '`Costo$`',             8,  'NumeroParte',      '%s',   'varchar(20)'],
                [10,    '`VentaUnit$`',         9,  'Descripcion',      '%s',   'varchar(30)'],
                [11,    '`Venta$`',             10, 'Cantidad',         '%s',   'decimal(15,4)'],
                [12,    '`Utilidad$`',          11, '`VentaUnit$`',     '%s',   'decimal(18,4)'],
                [13,    'Margen',               12, '`Venta$`',         '%s',   'decimal(35,10)'], 
                [14,    'TipoOrden',            13, '`CostoUnit$`',     '%s',   'decimal(18,4)'],
                [15,    'Taller',               14, '`Costo$`',         '%s',   'decimal(35,10)'],
                ['',    '',                     15, '`Utilidad$`',      '%s',   'decimal(18,4)'],
                ['',    '',                     16, 'Margen',           '%s',   'decimal(15,4)'],
                ]
        
        for m in model:
            writer.writerow(m)

def refser():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\refser01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                  1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Factura',             2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaFactura',        3,  'Factura',          '%s',   'varchar(15)'],
                [5,     'NumeroOT',            4,  'FechaFactura',     '%s',   'date'],
                [6,     'NumeroParte',         5,  'NumeroParte',      '%s',   'varchar(20)'],
                [7,     'Descripcion',         6,  'Descripcion',      '%s',   'varchar(30)'],
                [8,     'Cantidad',            7,  'Cantidad',         '%s',   'decimal(15,4)'],
                [9,     '`CostoUnit$`',        8,  '`VentaUnit$`',     '%s',   'decimal(18,4)'],
                [10,    '`Costo$`',            9,  '`Venta$`',         '%s',   'decimal(35,10)'],
                [11,    '`VentaUnit$`',        10, '`CostoUnit$`',     '%s',   'decimal(18,4)'],
                [12,    '`Venta$`',            11, '`Costo$`',         '%s',   'decimal(35,10)'],
                [13,    '`Utilidad$`',         12, '`Utilidad$`',      '%s',   'decimal(18,4)'], 
                [14,    'Margen',              13, 'Margen',           '%s',   'decimal(15,4)'],
                [15,    'TipoOrden',           14, 'RFC',              '%s',   'varchar(13)'],
                [16,    'Taller',              15, 'NumeroOT',         '%s',   'varchar(10)'],
                [17,    'RFC',                 16, 'TipoOrden',        '%s',   'varchar(30)'],                
                ['',    '',                    17, 'Taller',           '%s',   'varchar(30)'],
                ]
        
        for m in model:
            writer.writerow(m)


def seroep():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\seroep01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                  '',  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'NumeroOT',            '',  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaApertura',       '',  'Date',             '%s',   'date'],
                [5,     'Vin',                 '',  'NumeroOT',         '%s',   'varchar(10)'],
                [6,     'Dias',                '',  'Vin',              '%s',   'varchar(17)'],
                [7,     '`Costo$`',            '',  'FechaApertura',    '%s',   'date'],
                [8,     '`Venta$`',            '',  'Dias',             '%s',   'int(11)'],
                [9,     'TipoOrden',           '',  'Taller',           '%s',   'varchar(30)'],
                [10,    'Taller',              '',  'TipoOrden',        '%s',   'varchar(30)'],
                [11,    'NumeroAsesor',        '', '`Venta$`',          '%s',   'decimal(35,10)'],
                [12,    'NombreAsesor',        '', '`Costo$`',          '%s',   'decimal(35,10)'],
                [13,    'NombreCliente',       '', 'NumeroAsesor',      '%s',   'varchar(10)'], 
                [14,    'Direccion',           '', 'NombreAsesor',      '%s',   'varchar(30)'],
                [15,    'Telefono',            '', 'NombreCliente',     '%s',   'varchar(30)'],
                [16,    'CP',                  '', 'Direccion',         '%s',   'varchar(80)'],
                ['',    '',                    '', 'Telefono',          '%s',   'varchar(15)'],                
                ['',    '',                    '', 'CP',                '%s',   'char(5)'],
                ]
        
        for m in model:
            writer.writerow(m)

def sertec():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\sertec01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                  1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'NumeroOT',            2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'CodigoOperacion',     3,  'NumeroOT',         '%s',   'varchar(10)'],
                [5,     'FechaCierre',         4,  'CodigoOperacion',  '%s',   'varchar(15)'],
                [6,     'HorasPagadas',        5,  'FechaCierre',      '%s',   'date'],
                [7,     'HorasFacturadas',     6,  'Taller',           '%s',   'varchar(30)'],
                [8,     'NumeroMecanico',      7,  'TipoOrden',        '%s',   'varchar(30)'],
                [9,     'NombreMecanico',      8,  'HorasPagadas',     '%s',   'decimal(15,4)'],
                [10,    'Taller',              9,  'HorasFacturadas',  '%s',   'decimal(35,10)'],
                [11,    'TipoOrden',           10, 'NumeroMecanico',   '%s',   'varchar(10)'],
                ['',    '',                    11, 'NombreMecanico',   '%s',   'varchar(30)'],
              ]
        
        for m in model:
            writer.writerow(m)

def servta():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\servta01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',                     1,  'Client',                 '%s',   'smallint(5) unsigned'],
                [3,     'Factura',                2,  'Branch',                 '%s',   'smallint(5) unsigned'],
                [4,     'FechaFactura',           3,  'FechaFactura',           '%s',   'date'],
                [5,     'NumeroOT',               4,  'FechaApertura',          '%s',   'date'],
                [6,     'FechaApertura',          5,  'Dias',                   '%s',   'int(11)'],
                [7,     'FechaEntrega',           6,  'Factura',                '%s',   'varchar(15)'],
                [8,     'CodigoOperacion',        7,  'Taller',                 '%s',   'varchar(30)'],
                [9,     'Descripcion',            8,  'TipoOrden',              '%s',   'varchar(30)'],
                [10,    'TipoOperacion',          9,  'TipoPago',               '%s',   'varchar(30)'],
                [11,    '`CostoMO$`',             10, 'NumeroOT',               '%s',   'varchar(10)'],
                [12,    '`CostoMateriales$`',     11, 'VentasNetas',            '%s',   'tinyint(4)'],
                [13,    '`CostoTOT$`',            12, 'CodigoOperacion',        '%s',   'varchar(15)'],
                [14,    '`CostoPartes$`',         13, 'Descripcion',            '%s',   'varchar(30)'],
                [15,    '`VentaMO$`',             14, '`Venta$`',               '%s',   'decimal(35,10)'],
                [16,    '`VentaMateriales$`',     15, '`Costo$`',               '%s',   'decimal(35,10)'],
                [17,    '`VentaTOT$`',            16, '`Utilidad$`',            '%s',   'decimal(18,4)'],
                [18,    '`VentaPartes$`',         17, 'Margen',                 '%s',   'decimal(15,4)'],
                [19,    '`DescuentoMO$`',         18, '`VentaMO$`',             '%s',   'decimal(18,4)'],
                [20,    '`DescuentoMateriales$`', 19, '`DescuentoMO$`',         '%s',   'decimal(18,4)'],
                [21,    '`DescuentoTOT$`',        20, '`CostoMO$`',             '%s',   'decimal(18,4)'],
                [22,    '`DescuentoPartes$`',     21, '`VentaMateriales$`',     '%s',   'decimal(18,4)'],
                [23,    '`CostoTotal$`',          22, '`DescuentoMateriales$`', '%s',   'decimal(18,4)'],
                [24,    '`VentaTotal$`',          23, '`CostoMateriales$`',     '%s',   'decimal(18,4)'],
                [25,    '`Utilidad$`',            24, '`VentaTOT$`',            '%s',   'decimal(18,4)'],
                [26,    'Margen',                 25, '`DescuentoTOT$`',        '%s',   'decimal(18,4)'],
                [27,    'Taller',                 26, '`CostoTOT$`',            '%s',   'decimal(18,4)'],
                [28,    'NumeroAsesor',           27, '`VentaPartes$`',         '%s',   'decimal(18,4)'],
                [29,    'NombreAsesor',           28, '`DescuentoPartes$`',     '%s',   'decimal(18,4)'],
                [30,    'NombreCliente',          29, '`CostoPartes$`',         '%s',   'decimal(18,4)'],
                [31,    'RFC',                    30, '`VentaTotal$`',          '%s',   'decimal(35,10)'],
                [32,    'Direccion',              31, '`CostoTotal$`',          '%s',   'decimal(18,4)'],
                [33,    'Telefono',               32, 'NumeroAsesor',           '%s',   'varchar(10)'],
                [34,    'CP',                     33, 'NombreAsesor',           '%s',   'varchar(30)'],
                [35,    'Email',                  34, 'RFC',                    '%s',   'varchar(13)'],
                [36,    'Odometro',               35, 'NombreCliente',          '%s',   'varchar(30)'],
                [37,    'Vin',                    36, 'Direccion',              '%s',   'varchar(80)'],
                [38,    'Ano',                    37, 'Telefono',               '%s',   'varchar(15)'],
                [39,    'Marca',                  38, 'CP',                     '%s',   'char(5)'],
                [40,    'Modelo',                 39, 'Email',                  '%s',   'varchar(40)'],
                [41,    'Color',                  40, 'Odometro',               '%s',   'decimal(15,4)'],
                [42,    'Interior',               41, 'Vin',                    '%s',   'varchar(17)'],
                [43,    'TipoOrden',              42, 'Ano',                    '%s',   'int(11)'],
                [44,    'TipoPago',               43, 'Marca',                  '%s',   'varchar(10)'],
                ['',    '',                       44, 'Modelo',                 '%s',   'varchar(30)'],
                ['',    '',                       45, 'Color',                  '%s',   'varchar(30)'],
                ['',    '',                       46, 'Interior',               '%s',   'varchar(15)'],
                ['',    '',                       47, 'FechaEntrega',           '%s',   'date'],
                ['',    '',                       48, 'TipoOperacion',          '%s',   'varchar(30)'], 
              ]
        
        for m in model:
            writer.writerow(m)

def vtanue():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\vtanue01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',               1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Factura',          2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaFactura',     3,  'Factura',          '%s',   'varchar(15)'],
                [5,     'TipoVenta',        4,  'FechaFactura',     '%s',   'date'],
                [6,     'TipoPago',         5,  'TipoVenta',        '%s',   'varchar(30)'],
                [7,     'Vin',              6,  'TipoPago',         '%s',   'varchar(30)'],
                [8,     'NumeroInventario', 7,  'Vin',              '%s',   'varchar(17)'],
                [9,     '`Isan$`',          8,  'NumeroInventario', '%s',   'varchar(10)'],
                [10,    '`Costo$`',         9,  '`Isan$`',          '%s',   'decimal(18,4)'],
                [11,    '`Venta$`',         10, '`Costo$`',         '%s',   'decimal(35,10)'],
                [12,    '`Utilidad$`',      11, '`Venta$`',         '%s',   'decimal(35,10)'],
                [13,    'Margen',           12, '`Utilidad$`',      '%s',   'decimal(18,4)'], 
                [14,    'Ano',              13, 'Margen',           '%s',   'decimal(15,4)'], 
                [15,    'Marca',            14, 'Ano',              '%s',   'int(11)'], 
                [16,    'Modelo',           15, 'Marca',            '%s',   'varchar(10)'], 
                [17,    'Color',            16, 'Modelo',           '%s',   'varchar(30)'], 
                [18,    'Interior',         17, 'Color',            '%s',   'varchar(30)'], 
                [19,    'NumeroVendedor',   18, 'Interior',         '%s',   'varchar(15)'], 
                [20,    'NombreVendedor',   19, 'NumeroVendedor',   '%s',   'varchar(10)'], 
                [21,    'FechaCompra',      20, 'NombreVendedor',   '%s',   'varchar(30)'], 
                [22,    'FechaEntrega',     21, 'FechaCompra',      '%s',   'date'], 
                [23,    'NombreCliente',    22, 'FechaEntrega',     '%s',   'date'], 
                [24,    'RFC',              23, 'NombreCliente',    '%s',   'varchar(30)'], 
                [25,    'Direccion',        24, 'RFC',              '%s',   'varchar(15)'], 
                [26,    'Telefono',         25, 'Direccion',        '%s',   'varchar(80)'], 
                [27,    'CP',               26, 'Telefono',         '%s',   'varchar(15)'], 
                [28,    'Email',            27, 'CP',               '%s',   'char(5)'], 
                ['',    '',                 28, 'Email',            '%s',   'varchar(40)'], 
                ['',    '',                 29, 'VentasNetas',      '%s',   'tinyint(4)'], 
            
              ]
        
        for m in model:
            writer.writerow(m)


def vtausa():
    client = p.bclient()
    path = "C:\\Users\\simetrical\\Clients" + '\\' + '_' + client + '\\Imports\\vtausa01.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        model = [[1,     "NombreColumna",    "Separador", "col",     "col2", "dato"],
                [2,     'F1',               1,  'Client',           '%s',   'smallint(5) unsigned'],
                [3,     'Factura',          2,  'Branch',           '%s',   'smallint(5) unsigned'],
                [4,     'FechaFactura',     3,  'Factura',          '%s',   'varchar(15)'],
                [5,     'TipoVenta',        4,  'FechaFactura',     '%s',   'date'],
                [6,     'TipoPago',         5,  'TipoVenta',        '%s',   'varchar(30)'],
                [7,     'Vin',              6,  'TipoPago',         '%s',   'varchar(30)'],
                [8,     'NumeroInventario', 7,  'Vin',              '%s',   'varchar(17)'],
                [9,     '`Isan$`',          8,  'NumeroInventario', '%s',   'varchar(10)'],
                [10,    '`Costo$`',         9,  '`Isan$`',          '%s',   'decimal(18,4)'],
                [11,    '`Venta$`',         10, '`Costo$`',         '%s',   'decimal(35,10)'],
                [12,    '`Utilidad$`',      11, '`Venta$`',         '%s',   'decimal(35,10)'],
                [13,    'Margen',           12, '`Utilidad$`',      '%s',   'decimal(18,4)'], 
                [14,    'Ano',              13, 'Margen',           '%s',   'decimal(15,4)'], 
                [15,    'Marca',            14, 'Ano',              '%s',   'int(11)'], 
                [16,    'Modelo',           15, 'Marca',            '%s',   'varchar(10)'], 
                [17,    'Color',            16, 'Modelo',           '%s',   'varchar(30)'], 
                [18,    'Interior',         17, 'Color',            '%s',   'varchar(30)'], 
                [19,    'NumeroVendedor',   18, 'Interior',         '%s',   'varchar(15)'], 
                [20,    'NombreVendedor',   19, 'NumeroVendedor',   '%s',   'varchar(10)'], 
                [21,    'FechaCompra',      20, 'NombreVendedor',   '%s',   'varchar(30)'], 
                [22,    'FechaEntrega',     21, 'FechaCompra',      '%s',   'date'], 
                [23,    'NombreCliente',    22, 'FechaEntrega',     '%s',   'date'], 
                [24,    'RFC',              23, 'NombreCliente',    '%s',   'varchar(30)'], 
                [25,    'Direccion',        24, 'RFC',              '%s',   'varchar(15)'], 
                [26,    'Telefono',         25, 'Direccion',        '%s',   'varchar(80)'], 
                [27,    'CP',               26, 'Telefono',         '%s',   'varchar(15)'], 
                [28,    'Email',            27, 'CP',               '%s',   'char(5)'], 
                ['',    '',                 28, 'Email',            '%s',   'varchar(40)'], 
                ['',    '',                 29, 'VentasNetas',      '%s',   'tinyint(4)'], 
            
              ]
        
        for m in model:
            writer.writerow(m)

    
def fsm_core():
    invnue()
    invusa()
    refcom()
    refinv()
    refmos()
    refoep()
    refser()
    seroep()
    sertec()
    servta()
    vtanue()
    vtausa()


if __name__ == "__main__":
    fsm_core()
