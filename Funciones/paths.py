from pathlib import Path
import requests
from Funciones import Descompress as d

def bclient():

    c = open (d.sandbox + 'client.txt','r')
    client = c.read()
    c.close()

    return client

def bbranch():
   
    c = open (d.sandbox + 'branch.txt','r')
    client = c.read()
    c.close()
    
    return client

def path(i):
    path = d.path + i + '.txt'

    return path

def pathimp(i,ruta):
    pathImp= d.pathimp + ruta + '/Imports/' + i + '.csv'

    return pathImp

def pathexp(i,ruta):
    pathExp= d.pathimp + ruta + '/Exports/' + i + '.csv'

    return pathExp