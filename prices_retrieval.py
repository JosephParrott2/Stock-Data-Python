# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 21:42:51 2020

@author: wicke
"""

from __future__ import print_function

import datetime
import warnings
import mysql.connector
import csv

import MySQLdb as mdb
import requests

import yfinance as yf
import pandas as pd
import itertools

db_host = 'localhost'
db_user = 'root'
db_pass = 'password'
db_name = 'securities_master'   
connection = mysql.connector.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)


def obtain_list_of_db_tickers():
    
    cur = connection.cursor()
    cur.execute("SELECT id, ticker FROM symbol")
    data = cur.fetchall()
    return [d[1] for d in data]
    
def download_daily_historic_data_yahoo(symbol = obtain_list_of_db_tickers): 
    
    i = 0
    while i < len(symbol):
        data_df = yf.download(symbol[i], start= "2000-01-01", end= datetime.date.today())
        data_df.insert(0,"symbol_id", symbol[i])
        data_df.to_csv(symbol[i] + ".csv", header = False)    
        i += 1
        
def get_daily_historic_data_yahoo(ticker, start_date=(2000,1,1), end_date=datetime.date.today().timetuple()[0:3]):
    
    # Creating table of historic data for inputted stock
    try:
        read_string = "C:/Users/wicke/.spyder-py3/" + ticker + ".csv"
        with open(read_string, newline='') as csvfile:
            yf_data = csv.reader(csvfile)
            for row in yf_data:
                for e in row:
                    prices.append(e)           
    except Exception as e:
        print("Could not download Yahoo data: %s" %e)
    
    return prices
   
def insert_daily_prices(d):
    
    column_str = """price_date, symbol_id, open_price, high_price, low_price, close_price, adj_close_price, volume"""
    insert_str = ("%s, " * 8)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)
    
    cursor = connection.cursor()
    # The below three lines ensure the data is inserted in empty table
    cursor.execute("CREATE TABLE daily_price_copy LIKE daily_price")
    cursor.execute("DROP TABLE daily_price")
    cursor.execute("RENAME TABLE daily_price_copy to daily_price")
        
    # Inserting the list of data in the MySQL table
    cursor.executemany(final_str, d)
    connection.commit()
    print("Daily prices were successfully added.")
  

if __name__ == "__main__":
    
    prices = []
    download_daily_historic_data_yahoo(obtain_list_of_db_tickers())
     
    for t in enumerate(obtain_list_of_db_tickers()):
                
        test_prices = get_daily_historic_data_yahoo(t[1])
    
it = iter(test_prices)
prices_list_of_tuples = list(zip(it, it, it, it, it, it, it, it))

insert_daily_prices(prices_list_of_tuples)

