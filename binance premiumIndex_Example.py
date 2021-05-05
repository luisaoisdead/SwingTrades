# -*- coding: utf-8 -*-
"""
Created on Mon May  3 19:49:09 2021

@author: LuisaoRocks
"""

import requests

moneda1='btc'
moneda2='usdt'
#Creo la variable Symbol
symbol=moneda1+moneda2

url='https://fapi.binance.com/fapi/v1/premiumIndex'

#Inicio Bajada
params = {'symbol':symbol}
r = requests.get(url, params=params)
js = r.json()
print(js)