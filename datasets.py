import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

import sys
#sys.path.append('{YOURPATH})
#import etl
#etl.executeEtl()

def datasetsToSqlToCsv():
    try:
        conn = msql.connect(host="127.0.0.1", user="root", password="", database='pruebas')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record) 
                
            SQL_Query_CatArt = pd.read_sql_query('''SELECT * FROM catArt''', conn)
            dataFrameCarArt = pd.DataFrame(SQL_Query_CatArt, columns=['id_art', 'id_clasif'])
            print(dataFrameCarArt)
            
            SQL_Query_compras = pd.read_sql_query('''SELECT * FROM compras''', conn)
            dataFrameCompras = pd.DataFrame(SQL_Query_compras, columns=['id_cte', 'id_art'])
            print(dataFrameCompras)
            
            dataFrameOutset = pd.merge(dataFrameCarArt, dataFrameCompras, on='id_art', how='inner')
           
            dataFrameOutset.to_csv('/home/ale/Desktop/workspace/examen/final.csv')
    except Error as e:
        print("Error while connecting to MySQL", e)
    
datasetsToSqlToCsv()