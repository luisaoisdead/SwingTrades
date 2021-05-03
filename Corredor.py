# -*- coding: utf-8 -*-

"""
Created on Wed Jan 27 16:08:54 2021
@author: LuisaoRocks
"""


import keys
usuario=keys.usuario
password=keys.password

import pandas as pd
import requests
import json

#Funcion Guardado en DB
from sqlalchemy import create_engine
import db

# Hilos
import threading
import time


#Inicio Horario
from pytz import timezone
import datetime

#mokCotizaciones
import random as rd

#Variables, que me permite la interaccion entre los hilos.
global ultimo_dato
ultimo_dato={}
global contador
contador=0


HORA_INICIO_BOT = 11.55
HORA_FIN_BOT = 17.0


tz=timezone('America/Argentina/Buenos_Aires')

def logueo(usuario, contrasena):
        global r
        r =  json.loads(requests.post("https://api.invertironline.com/token",data={
            "username":usuario,
            "password":contrasena,
            "grant_type":"password"        
        }).text)["access_token"]
        print(f'Estoy conectado y mi token es:\n{r}')

def logueoInf(usuario, contrasena):
    global fueraDeHorario
    while True:
        logueo(usuario,contrasena)
        segundos=60*14
        time.sleep(segundos)
        if(fueraDeHorario):
            break

def cotizaciones(simbolo,plazo):
    global r
    cot =  json.loads(requests.get(
        url = "https://api.invertironline.com//api/v2/bCBA/Titulos/"+simbolo+"/Cotizacion?Plazo="+plazo,
        headers = {"Authorization":"Bearer "+r} 
        ).text)
    cot.update({'plazo':plazo})
    return cot

#Solo lo uso para probar las funciones.
def mokCotizaciones(simbolo,plazo):

    global contador
    ahora = datetime.datetime.now(tz=tz)
    
    cot={'ultimoPrecio': (5718.75+rd.randrange(0,5,1)), 'variacion': 0.0, 
            'apertura': (5680.0+rd.randrange(0,5,1)), 'maximo': (5687.0++rd.randrange(0,5,1)), 'minimo': (5649.0+rd.randrange(0,5,1)), 
            'fechaHora': ahora, 
            'tendencia': 'sube', 'cierreAnterior': (5787.39+rd.randrange(0,5,1)), 
            'montoOperado': 4285545347.16, 'volumenNominal': (75208027+rd.randrange(0,10000)), 
            'precioPromedio': 0.0, 'moneda': 'mokCotizaciones', 
            'precioAjuste': 0.0, 'interesesAbiertos': 0.0, 
            'puntas': [{'cantidadCompra': (97546.0+rd.randrange(0,1000)), 'precioCompra': (5650.5+rd.randrange(0,5,1)), 
                        'precioVenta': (5665.0+rd.randrange(0,5,1)), 'cantidadVenta': (93385.0+rd.randrange(0,10000))}, 
                       {'cantidadCompra': 28684.0, 'precioCompra': (5650.0+rd.randrange(0,5,1)), 
                        'precioVenta': (5669.0+rd.randrange(0,5,1)), 'cantidadVenta': 62803.0}, 
                       {'cantidadCompra': 315.0, 'precioCompra': (5649.0+rd.randrange(0,5,1)), 
                        'precioVenta': (5669.5+rd.randrange(0,5,1)), 'cantidadVenta': 52955.0}, 
                       {'cantidadCompra': 3476.0, 'precioCompra': (5646.0+rd.randrange(0,5,1)), 
                        'precioVenta': (5670.0+rd.randrange(0,5,1)), 'cantidadVenta': 4896.0}, 
                       {'cantidadCompra': 13949.0, 'precioCompra': (5645.0+rd.randrange(0,5,1)), 
                        'precioVenta': (5672.0+rd.randrange(0,5,1)), 'cantidadVenta': 5003.0}], 
                        'cantidadOperaciones': 49}
    cot.update({'plazo':plazo})
    return cot
    
def GuardoDB(data,broker='iol',modo='bloque',simbolo=''):
    # conexion a la DB
    db_connection = create_engine(db.DB_CONNECTION)
#    conn = db_connection.connect()

    # creo la tabla
    create_table = f'''
        CREATE TABLE IF NOT EXISTS `{broker}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `simbolo` varchar(20) DEFAULT '',
          `fechahora` timestamp NULL DEFAULT NULL,
          `ultimoprecio` float(20) DEFAULT NULL,
          `apertura` float(10) DEFAULT NULL,
          `cierreanterior` float(10) DEFAULT NULL,
          `maximo` float(10) DEFAULT NULL,
          `minimo` float(10) DEFAULT NULL,
          `montooperado` int(20) NOT NULL,          
          `cantidadoperaciones` float(10) DEFAULT NULL,
          `volumennominal` float(10) DEFAULT NULL,
          `puntacompracantidad` int(20) NOT NULL,
          `puntapreciocompra` float(10) DEFAULT NULL,
          `puntaventacantidad` int(20) NOT NULL,
          `puntaprecioventa` float(10) DEFAULT NULL,
          `plazo` varchar(2) DEFAULT '',
          PRIMARY KEY (`id`),
          UNIQUE KEY `idx_ticker_tradeid` (`simbolo`,`fechahora`,`plazo`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
        '''

    db_connection.execute(create_table)
    if(modo=='bloque'):  
        try:
            data.to_sql(con=db_connection, name=broker, if_exists='append',index=False)
        except:
            print('Fallo el guardado en la DB, en modo Bloque')

    elif(modo=='linea'):
        try:
            insertar = f'''
                INSERT INTO `{broker}` 
                    (`simbolo`, `fechahora`, `ultimoprecio`, `apertura`, 
                     `cierreanterior`, `maximo`, `minimo`, `montooperado`, 
                     `cantidadoperaciones`, `volumennominal`, 
                     `puntacompracantidad`, `puntapreciocompra`, 
                     `puntaventacantidad`, `puntaprecioventa`,
                     `plazo`)
                    VALUES ('{simbolo}','{data['fechaHora']}',{data['ultimoPrecio']},{data['apertura']},
                            {data['cierreAnterior']},{data['maximo']},{data['minimo']},{data['montoOperado']},
                            {data['cantidadOperaciones']},{data['volumenNominal']},
                            {data['puntas'][0]['cantidadCompra']},{data['puntas'][0]['precioCompra']},
                            {data['puntas'][0]['cantidadVenta']},{data['puntas'][0]['precioVenta']},
                            '{data['plazo']}');
                '''
            try:
                db_connection.execute(insertar)            
            except:
                print(f'Fallo el guardado en la DB, en modo Linea en el simbolo: {simbolo}')
        except:
            print(f'Fallo la generacion de la sentencia SQL, en modo Linea en el simbolo: {simbolo}')

def precio(ultimo_dato,par):

    #Ordeno Datos
    compraPesosCantidad=    ultimo_dato[par[0]]['puntas'][0]['cantidadCompra']
    compraPesosPrecio=      ultimo_dato[par[0]]['puntas'][0]['precioCompra']
    ventaPesosCantidad=     ultimo_dato[par[0]]['puntas'][0]['cantidadVenta']
    ventaPesosPrecio=       ultimo_dato[par[0]]['puntas'][0]['precioVenta']
   
    compraUsdCantidad=  ultimo_dato[par[1]]['puntas'][0]['cantidadCompra']
    compraUsdPrecio=    ultimo_dato[par[1]]['puntas'][0]['precioCompra']
    ventaUsdCantidad=   ultimo_dato[par[1]]['puntas'][0]['cantidadVenta']
    ventaUsdPrecio=     ultimo_dato[par[1]]['puntas'][0]['precioVenta']
   
    #Calculo
    compraUSD=      ventaUsdPrecio/compraPesosPrecio 
    cantidadUSD=    min(ventaUsdCantidad, compraPesosCantidad)
    compraPesos=    ventaPesosPrecio/compraUsdPrecio
    cantidadPesos=  min(ventaPesosCantidad, compraUsdCantidad)
    '''
    print(f'Estoy en el par: {par}.', 
          f'\nEl precio de compra de usd es: {compraUSD} por {cantidadUSD}',
          f'\nEl precio de compra de pesos es: {compraPesos} por {cantidadPesos}')
    '''
    dev={f'{par[0]}-{par[1]}':{'preciousd':compraUSD,'cantidadusd':cantidadUSD,
         'preciopesos':compraPesos,'cantidadpesos':cantidadPesos}}
        
    return dev        
        
def armoPanelBonos(pares,demo=False):
    #Creo la variable global
    global ultimo_dato
    global contador
    #panel={}
    #Armo el Panel
    
    for par in pares:
        for bono in par:
            try:
                # Ejemplo: Inmediata = t0, plazo normal (48):  t2, t1 en bonos, no tiene puntas
                plazos=('t0','t2')
                for plazo in plazos:
                    if(demo):
                        #Solo lo uso para probar las funciones.
                        valor=mokCotizaciones(bono,plazo)
                    else:
                        #Valores reales de IOL
                        valor=cotizaciones(bono, plazo)
                    
                    '''
                    ahora = datetime.datetime.now(tz=tz)
                    hora_decimal = round(ahora.hour + ahora.minute/60 + ahora.second/3600,5)
                    nombre=bono+'-'+str(hora_decimal)
                    ultimo_dato.update({nombre:valor})
                    '''
                    #Solo un plazo.
                    #ultimo_dato.update({bono:valor})
                    #Mas de un plazo.
                    ultimo_dato.update({f'{bono}-{plazo}':valor})
                    
              
            except:
                pass
                print(f'Fallo el armado del panel con el par {par}, y el bono {bono}\n ultimo_dato es {ultimo_dato}')
        
        #Busco Oportunidades
        #try:
        #    preciopar=precio(ultimo_dato=ultimo_dato,par=par)
        #    panel.update(preciopar)
        #except:
        #    pass
    #mejorComprausd=min([value['preciousd'] for value in panel.values()])
    #mejorComprapesos=min([value['preciopesos'] for value in panel.values()])
    
    #print(panel,mejorComprausd,mejorComprapesos)        

    
 
def armoPanelBonosInf(pares,demo=False):
    global ultimo_dato
    global contador
    global fueraDeHorario
    contador=0
    while True:
        armoPanelBonos(pares,demo=demo)
        #print('Pido Armo Panel Bonos')
        segundos=60*1
        time.sleep(segundos)        
        if(fueraDeHorario):
            break
        
def conviertoDF(ultima_base):
    #Preparo el DF, desde el diccionario.
    df=pd.DataFrame([key for key in ultima_base.keys()],columns=['simbolo'])
    df['apertura']=[value['apertura'] for value in ultima_base.values()]
    df['cantidadoperaciones']=[value['cantidadOperaciones'] for value in ultima_base.values()]
    df['cierreanterior']=[value['cierreAnterior'] for value in ultima_base.values()]
    df['fechahora']=[value['fechaHora'] for value in ultima_base.values()]
    df['interesesabiertos']=[value['interesesAbiertos'] for value in ultima_base.values()]
    df['maximo']=[value['maximo'] for value in ultima_base.values()]
    df['minimo']=[value['minimo'] for value in ultima_base.values()]
    df['moneda']=[value['moneda'] for value in ultima_base.values()]
    df['montooperado']=[value['montoOperado'] for value in ultima_base.values()]
    df['precioajuste']=[value['precioAjuste'] for value in ultima_base.values()]
    df['preciopromedio']=[value['precioPromedio'] for value in ultima_base.values()]
    df['puntacompracantidad']=[value['puntas'][0]['cantidadCompra'] for value in ultima_base.values()]
    df['puntapreciocompra']=[value['puntas'][0]['precioCompra'] for value in ultima_base.values()]
    df['puntaventacantidad']=[value['puntas'][0]['cantidadVenta'] for value in ultima_base.values()]
    df['puntaprecioventa']=[value['puntas'][0]['precioVenta'] for value in ultima_base.values()]
    df['tendencia']=[value['tendencia'] for value in ultima_base.values()]
    df['ultimoprecio']=[value['ultimoPrecio'] for value in ultima_base.values()]
    df['variacion']=[value['variacion'] for value in ultima_base.values()]
    df['volumennominal']=[value['volumenNominal'] for value in ultima_base.values()]
    df['plazo']=[value['plazo'] for value in ultima_base.values()]
    #convierto la fecha
    #df['time'] = pd.to_datetime(df.time_ms, unit='ms')
    # Elimino columnas que no quiero
    df.drop(['interesesabiertos','precioajuste','preciopromedio','tendencia',
             'variacion','moneda'],axis=1,inplace=True)
    #Elimino los duplicados
    df.drop_duplicates()
    return df

def guardo(modo='bloque'):
    global ultimo_dato
    if (ultimo_dato!={}):
        #copio el diccionario, esto me permite seguir sumando datos a ultimo_dato
        ultima_base=ultimo_dato.copy()
        if(modo=='bloque'):  
            
            #Convierto el diccionario en DF
            df=conviertoDF(ultima_base)    
            
            #Guardo en la base de datos.
            GuardoDB(data=df)

            #Elimino en ultimo_dato, los tradeid ya guardados.
            for key in ultima_base.keys(): del ultimo_dato[key]
        
        elif(modo=='linea'):
            #Recorr
            for key in ultima_base.keys(): 
                #guardolinea 
                GuardoDB(data=ultima_base[key],modo='linea',simbolo=key)
                
                #Elimino la linea guardada
                del ultimo_dato[key]
            


def guardoInf():
    while True:
        #print('Guardo en DB')
        guardo(modo='linea')
        #cada 5 minutos guardo info.
        segundos=60*1
        time.sleep(segundos)
        if(fueraDeHorario):
            break

def mainInf():
    global fueraDeHorario
    fueraDeHorario= True
    hilo1 = threading.Thread(target=logueoInf,args=[usuario,password])
    hilo2 = threading.Thread(target=armoPanelBonosInf,args=[pares]) 
    hilo3 = threading.Thread(target=guardoInf)
    
    while True:
        ahora = datetime.datetime.now(tz=tz)
        hora_decimal = round(ahora.hour + ahora.minute/60 + ahora.second/3600,5)
        
        if hora_decimal < HORA_INICIO_BOT:
            print(f'Esperando a las {HORA_INICIO_BOT}, Son las:',hora_decimal)
            time.sleep(30)
            # Aca puede ejecutar tareas de preparacion de rueda
            fueraDeHorario=True
            
        elif ((hora_decimal > HORA_INICIO_BOT) & (hora_decimal < HORA_FIN_BOT) & fueraDeHorario==True):
            #Ejecutamos el programa
            hilo1.start()
            #Necesito esperar unos segundos para poder tener el token
            time.sleep(5)
            hilo2.start()
            #Espero 10 segundos para comenzar a guardar la informacion.
            time.sleep(10)
            hilo3.start()
            fueraDeHorario=False
            esperar=int((HORA_FIN_BOT-hora_decimal)*3600)
            time.sleep(esperar)
            
        elif (hora_decimal > HORA_FIN_BOT):
            print(f'Son las {hora_decimal}, fin de horario de operaciones: {HORA_FIN_BOT}')
            time.sleep(30)
            # Aca puede ejecutar tareas de finalizacion del dia
            fueraDeHorario=True
            




#Casos de Uso.
if __name__ == '__main__':
    #Uso uno u otro, sino da un error.
       
    
    print("---------Defino Bonos----------")
    pares=[("AL30","AL30D"),("GD30","GD30D"),("GD35","GD35D"),("AE38","AE38D"),("AL35","AL35D"),("AL29","AL29D"),
           ("GD41","GD41D"),("AL41","AL41D")]

    #Hilo en productivo Con horarios.
    #mainInf()
    
    # 1 Hilo en productivo CON horarios.
    # 2 Hilo en productivo Sin horarios.
    # 3 Prueba funciones aisladas, con logueo.
    # 4 Prueba funciones aisladas, sin logueo.
    # 5 Prueba funciones aisladas en los hilos.
    
    tipo=1
    
    if(tipo==1):
        hilo0=threading.Thread(target=mainInf)
        hilo0.start()
        
    if(tipo==2):
        #Hilo en productivo Sin horarios.
        
        hilo1 = threading.Thread(target=logueoInf,args=[usuario,password])
        hilo2 = threading.Thread(target=armoPanelBonosInf,args=[pares]) 
        hilo3 = threading.Thread(target=guardoInf)
        hilo1.start()
        #Necesito esperar unos segundos para poder tener el token
        time.sleep(5)
        hilo2.start()
        #Espero 10 segundos para comenzar a guardar la informacion.
        time.sleep(10)
        hilo3.start()
    
    if(tipo==3):
        #Prueba funciones aisladas, con logueo.
        logueo(usuario,password)
       
        armoPanelBonos(pares,demo=False)
        guardo(modo='linea')
        #guardo(modo='bloque')

    if(tipo==4):
        #Prueba funciones aisladas, sin logueo.
        
        armoPanelBonos(pares,demo=True)
        guardo(modo='linea')
        #guardo(modo='bloque')

    if(tipo==5):    
        #Prueba funciones aisladas en los hilos.
                
        hilo2 = threading.Thread(target=armoPanelBonosInf,args=[pares,True]) 
        hilo3 = threading.Thread(target=guardoInf)
        hilo2.start()
        #Espero 10 segundos para comenzar a guardar la informacion.
        time.sleep(10)
        hilo3.start()
