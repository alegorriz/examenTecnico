import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

mainpath = "/home/ale/Desktop/workspace/examen/"
catArt = "CatArt.csv"
compras = "compras.csv"
promos = "Promos.csv"

"""This is the main fuction for connect, extract data from csv files, 
connect to a database, transform csv objects to sql objects, create
sql squema with it's relationships and show complete information from
database.
"""
def executeEtl():
    try:
        conn = msql.connect(host="127.0.0.1", user="root", password="", database='pruebas')
        if conn.is_connected():
          cursor = conn.cursor()
          cursor.execute("select database();")
          record = cursor.fetchone()
          print("You're connected to database: ", record)
          
          createTables(conn)
          loadData(conn)
          selectAndShowTables(conn)
          conn.close()
    except Error as e:
        print("Error while connecting to MySQL", e)

"""This function deletes and creates tables and it's realtionships
 into database.
"""
def createTables(conn):
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS compras;')
    cursor.execute('DROP TABLE IF EXISTS promos;')
    cursor.execute('DROP TABLE IF EXISTS catArt;')
    
    print('Creating tables....')
    
    cursor.execute("CREATE TABLE catArt(id_art INT PRIMARY KEY, id_clasif INT);")
    print("catArt table is created....")
    
    cursor.execute("CREATE TABLE compras(id_cte VARCHAR(10), id_art INT, FOREIGN KEY(id_art) REFERENCES catArt(id_art));")
    print("compras table is created....")
    
    cursor.execute("CREATE TABLE promos(id_art INT, desc_art VARCHAR(10), FOREIGN KEY(id_art) REFERENCES catArt(id_art));")
    print("promos table is created....")
        
"""
This function load information from csv fles into sql tables.
"""
def loadData(conn):    
    dataCatArt = pd.read_csv(mainpath + catArt)
    dataCompras = pd.read_csv(mainpath + compras)
    dataPromos = pd.read_csv(mainpath + promos)
    
    cursor = conn.cursor()
    
    for i,row in dataCatArt.iterrows():
        sql = "INSERT INTO catArt VALUES (%s,%s);"
        cursor.execute(sql, tuple(row))
        print("Record inserted in catArt")
        conn.commit()
    
    for i,row in dataCompras.iterrows():
        sql = "INSERT INTO compras VALUES (%s,%s);"
        cursor.execute(sql, tuple(row))
        print("Record inserted in compras")
        conn.commit()

    for i,row in dataPromos.iterrows():
        sql = "INSERT INTO promos VALUES (%s,%s);"
        cursor.execute(sql, tuple(row))
        print("Record inserted in promos")
        conn.commit()

"""
In order to display the information contained in the sql tables
this execute selects over several tables and display this information
in the cmd.
"""        
def selectAndShowTables(conn):
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to database: ", record)

    # Execute query
    sql = "SELECT * FROM catArt;"
    cursor.execute(sql)
    
    # Fetch all the records
    result = cursor.fetchall()
    for i in result:
        print(i)
    
    # Execute query
    sql = "SELECT * FROM compras;"
    cursor.execute(sql)
    
    # Fetch all the records
    result = cursor.fetchall()
    for i in result:
        print(i)
    
    # Execute query
    sql = "SELECT * FROM promos;"
    cursor.execute(sql)
    
    # Fetch all the records
    result = cursor.fetchall()
    for i in result:
        print(i)     
        
executeEtl()
