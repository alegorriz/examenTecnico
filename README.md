# Walmart Exam

## Question 1

You can see the separate querys in the next section:

    - CLIENTS PROMO SUBSET (1290)
    - CLIENTS WITHOUT PROMO SUBSET (647)
    - TOP OF NON-PROMOTIONAL PURCHASES WITH SAME CLASIFICATION (737)
    - TOP OF NON-PROMOTIONAL PURCHASES WITH DIFFERENT CLASIFICATION (834)
    - TOP OF PROMOTIONAL PURCHASES(2265)

### CLIENTS PROMO SUBSET (1290)

```sh
--SUBCONJUNTO CLIENTES PROMO (1290):

SELECT DISTINCT(c.id_cte)
FROM compras c, promos p
WHERE c.id_art=p.id_art;
```

### CLIENTS WITHOUT PROMO SUBSET (647)

```sh
--SUBCONJUNTO CLIENTES SIN PROMO (647):   [USO DE 2]

SELECT DISTINCT(id_cte)
FROM compras
WHERE id_cte
NOT IN(
    SELECT DISTINCT(c.id_cte)
    FROM compras c, promos p
    WHERE c.id_art=p.id_art
);
```

### TOP OF NON-PROMOTIONAL PURCHASES WITH SAME CLASIFICATION (737)

```sh
--TOP COMPRAS NO PROMOCIONALES CON MISMA CLASIFICACION (737)

SELECT c.id_cte, c.id_art, COUNT(c.id_art)
FROM compras c, catArt a
WHERE c.id_art = a.id_art
AND a.id_art
NOT IN(
    SELECT DISTINCT(id_art)
    FROM promos
)AND a.id_clasif
IN(
    SELECT DISTINCT(a.id_clasif)
    FROM catArt a, promos p
    WHERE a.id_art = p.id_art
)GROUP BY c.id_cte, c.id_art
ORDER BY c.id_cte, COUNT(c.id_art);
```

### TOP OF NON-PROMOTIONAL PURCHASES WITH DIFFERENT CLASIFICATION (834)

```sh
--TOP COMPRAS NO PROMOCIONALES CON DISTINTA CLASIFICACION (834)

SELECT c.id_cte, c.id_art, COUNT(c.id_art)
FROM compras c, catArt a
WHERE c.id_art = a.id_art
AND a.id_art
NOT IN(
    SELECT DISTINCT(id_art)
    FROM promos
)AND a.id_clasif
NOT IN(
    SELECT DISTINCT(a.id_clasif)
    FROM catArt a, promos p
    WHERE a.id_art = p.id_art
)GROUP BY c.id_cte, c.id_art
ORDER BY c.id_cte, COUNT(c.id_art);
```

### TOP OF PROMOTIONAL PURCHASES(2265)

```sh
--TOP COMPRAS PROMOCIONALES (2265)

SELECT c.id_cte, c.id_art, COUNT(c.id_art)
FROM compras c, catArt a
WHERE a.id_art = c.id_art
AND a.id_art
IN(
    SELECT DISTINCT(id_art)
    FROM promos
)GROUP BY c.id_cte, c.id_art
ORDER BY c.id_cte, COUNT(c.id_art);
```

### TABLE WITH TOTAL (3836)

```sh
-- TABLA DE TOP TOTAL (3836) (AQUI FALTA EL LIMIT PARA QUE SEAN SOLO 7)

SELECT id_cte, id_art, COUNT(id_art)
FROM compras
GROUP BY id_cte, id_art
ORDER BY id_cte, COUNT(id_art);
```



## Question 2

The steps for loading data from .csv into a database are:

    - Create de database in the system
    - Stablish a connection with some vendor of database with it's connector (in this case I used MySql
    - With the connection object of the last step create a connector object and select the database in order to excecute queries over the data base
    - Use the connector object to create the tables into the database passing as arguments the "create table" sql sentence (notice that I have created the relationships between the tables).
    - In this step I load all the data from the .csv files into the tables that I have created in the last step, running the "INSER INTO" sql setence into a for loop for each table.
    - As last step I put all together in python functions and to prove that the data was created I execute a "SELECT *" sql sentence over the three tables.
    
Finally I attach you the code with some documentation and some images:

You can see te ETL function in the next section:

    - Execute ETL Function
    - Create Tables Function
    - Load Data Function
    - Select And Show Tables Function
    
### Imports

```sh
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

mainpath = "{YourPath}"
catArt = "CatArt.csv"
compras = "compras.csv"
promos = "Promos.csv"
```

### Execute ETL function
This is the main fuction for connect, extract data from csv files, connect to a database, transform csv objects to sql objects, create sql squema with it's relationships and show complete information from database.

```sh
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
```
### Create Tables Function

This function deletes and creates tables and it's realtionships into database.

```sh
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
```
### Create Tables Function

This function load information from csv fles into sql tables.

```sh
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
```

### Load Data Function

This function load information from csv fles into sql tables.

```sh
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
```

### Select Tables And Show Tables Function

In order to display the information contained in the sql tables this execute selects over several tables and display this information in the cmd.

```sh
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
```

## Question 3

```sh
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

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
            
            dataFrameOutset = pd.merge(dataFrameCarArt, dataFrameCompras, on='id_art', how='outer')
           
            dataFrameOutset.to_csv('{YourPath}')
    except Error as e:
        print("Error while connecting to MySQL", e)
    
datasetsToSqlToCsv()
```



<img src="" width="100" height="100">











