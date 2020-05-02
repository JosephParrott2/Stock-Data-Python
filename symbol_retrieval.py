# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import datetime

import bs4
import requests
from bs4 import BeautifulSoup
import MySQLdb as mdb
import mysql.connector
from mysql.connector import Error


# Download and parse the Wikipedia list of S&P500.
def obtain_parse_wiki_snp500():
    
    now = datetime.datetime.utcnow()
    
    response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = bs4.BeautifulSoup(response.text, features='lxml')
    
    symbolslist = soup.select('table')[0].select('tr')[1:]
    
    
    symbols = []
    for i, symbol in enumerate(symbolslist):
        tds = symbol.select('td')
        symbols.append(
            (
                tds[0].select('a')[0].text,
                'stock',
                tds[1].select('a')[0].text,
                tds[3].text,
                'USD', now, now
                )
            )
    return symbols

# Inserting the S&P500 symbols into the MySQL database.
def insert_snp500_symbols(symbols):
    
    try:    
        db_host = 'localhost'
        db_user = 'root'
        db_pass = 'password'
        db_name = 'securities_master'   
        connection = mysql.connector.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)        # Conncting to MySQL database
        
        column_str = """ticker, instrument, name, sector, currency, created_date, last_updated_date """
        insert_str = ("%s, " * 7)[:-2]
        final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
        
        cursor = connection.cursor()
        # The below three lines ensure the data is inserted in empty table
        cursor.execute("CREATE TABLE table_copy LIKE symbol")
        cursor.execute("DROP TABLE symbol")
        cursor.execute("RENAME TABLE table_copy to symbol")
        
        # Inserting the list of data in the MySQL table
        cursor.executemany(final_str, symbols)
        connection.commit()
        print("Symbols were successfully added.")
        
    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
                cursor.close()
                connection.close()
                print("MySQL connection is closed.")
        
        
        
        
if __name__ == "__main__":
    
    symbols = obtain_parse_wiki_snp500()
    insert_snp500_symbols(symbols)

    
        
        