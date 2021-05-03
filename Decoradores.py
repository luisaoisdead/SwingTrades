# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 09:36:08 2021

@author: ltaboada

Python Decorators
https://www.programiz.com/python-programming/decorator

"""
import datetime
from pytz import timezone
import time

HORA_INICIO_BOT = 11.55
HORA_FIN_BOT = 17.0
tz=timezone('America/Argentina/Buenos_Aires')

def inc(x):
    return x + 1


def dec(x):
    return x - 1


def operate(func, x):
    ahora = datetime.datetime.now(tz=tz)
    hora_decimal = round(ahora.hour + ahora.minute/60 + ahora.second/3600,5)
    result = 0
    arranco=False
    if hora_decimal < HORA_INICIO_BOT:
        print(f'Esperando a las {HORA_INICIO_BOT}, Son las:',hora_decimal)
        time.sleep(1)
        # Aca puede ejecutar tareas de preparacion de rueda
        
    elif ((hora_decimal > HORA_INICIO_BOT) & (hora_decimal < HORA_FIN_BOT) & arranco==False):
        #Ejecutamos el programa
        result = func(x)    
        arranco=True
    elif (hora_decimal > HORA_FIN_BOT):
        print(f'Son las {hora_decimal}, fin de horario de operaciones: {HORA_FIN_BOT}')
    
    return result

operate(dec,5)