import psycopg2
import configparser
import os 
import sys
import petl 
import requests
import datetime
import json 
import decimal 
import sqlite3
config = configparser.ConfigParser()
try:
    config.read('ETLDEMO.ini')
except Exception as error:
    print('could not read CONFIG file:' + str(error))
    sys.exit()
try:
    conn = psycopg2.connect(
        host = config['CONFIG']['hostname'],
        dbname = config['CONFIG']['database'],
        user = config['CONFIG']['username'],
        password = config['CONFIG']['pwd'],
        port = config['CONFIG']['port_id'],
    )
    url = config['CONFIG']['url']
    startDate = config['CONFIG']['startDate']
    cur = conn.cursor()
except Exception as error:
    print(error)
    sys.exit()
try:
    BOCResponse = requests.get(url+startDate)
except Exception as error:
    print('could not make request:' + str(error))
    sys.exit()
BOCDates = []
BOCRates = []
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))
    exchangeRates = petl.fromcolumns([BOCDates,BOCRates],header=['date','rate'])
    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx',sheet='Github')
    except Exception as error:
        print('could not open expenses.xlsx:' + str(error))
        sys.exit()
    expenses = petl.outerjoin(exchangeRates,expenses,key='date')
    expenses = petl.filldown(expenses,'rate')
    expenses = petl.select(expenses,lambda rec: rec.USD != None)
    expenses = petl.addfield(expenses,'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)
    try:
        conn
    except Exception as error:
        print('could not connect to database:' + str(error))
        sys.exit()
    try:
        cur.execute("""INSERT INTO expenses ("date","rate","usd","cad") VALUES (date, rate, usd, cad);""",expenses)
    except Exception as error:
        print('could not write to database:' + str(error))
    print (expenses)
