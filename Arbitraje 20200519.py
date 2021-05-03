# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 13:08:57 2020

@author: JCSM
"""
# from tkinter import * # library for graphical interface
from IOL import *
import threading
import time

# raiz = TK()
# miFrame = Frame(raiz)
# miFrame.pack()
global Invertir
Invertir = IOL("Taboada_luis","Y6u7i8o9%")

#--------------------------------- functions ---------------------------------
def fecha():
    fecha=datetime.now()
    dia=fecha.day
    mes=fecha.month
    año=fecha.year
    dev=str(año)+"-"+str(mes)+"-"+str(dia)
    return dev

# def registrarEnCSV(fecha,simbolo,plazoInicial,plazoFinal,precioInicial,cantidadInicial,precioFinal,cantidadFinal,cantidadTransaccion):
#     fields=[fecha,simbolo,plazoInicial,plazoFinal,precioInicial,cantidadInicial,precioFinal,cantidadFinal,cantidadTransaccion]
#     with open(r'Oportunidades.csv', 'a') as f:
#         writer = csv.writer(f)
#         writer.writerow(fields) 

#----------------------------------- code ------------------------------------

def ingre():
    while True:
        time.sleep(50)
        print("---------Ingreso----------")
        Invertir = IOL("Taboada_luis","Y6u7i8o9%")
        print("---------Hash-------------")
        print(Invertir.r)
        

threads = []
t = threading.Thread(target=ingre)
threads.append(t)
t.start()

print("---------Estado de Cuenta----------")
print(Invertir.estadoDeCuenta())

print("---------Mi PortFolio----------")
print(Invertir.portafolio())

print("---------Busco Oportunidades----------")
Bonos=["AY24","AY24D","AF20","AF20D","AO20"]
while True:
    for i in Bonos:
        [mensaje, data] = Invertir.cotizacionesComparadas(i,"t0","t2")
        print(mensaje)
        
        if  data["Tomar_Operacion"][0] ==  "True":
            fechaOperacion = fecha()
            data = data.assign(Fecha=fechaOperacion) #Agrego una columna llamada Fecha al dataframe
            data.to_csv('Historico_Oportunidades.csv', mode='a')
    import time    
    print("------------------")
    time.sleep(0)
    

print("-------------------")
print("Fecha de operación: " + fecha())
   

'''
Acceder al dato de una tabla.
TablaCotizacion["puntas"][0]["cantidadCompra"]

Imprimir el token en la pantalla
IO.r

'''
   
# print(IO.cotizaciones())
# print(IO.intradia('AY24D'))


        
    
    
