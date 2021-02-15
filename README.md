# Walmart Exam

The following README file has the answers to the technical exam to be part of Walmart team.
In the first section I add all the queries, views, procedures and analysis made to answer the first exercise.
Then I add fragments of the script used to test ETL functions with python and pandas library, as you can see I attach the files in this repository.
Finally I add a final script to test and inner join with DataFrames.
In order to accomplish the test requirements I also attach in this repoitory a .csv file called "final.csv" where the answer to the last exercise is to be found.

## Question 1

You can see the separate querys in the next section:

    * CLIENTS WHO BOUGHT PROMOS (1290 rows)
    * CLIENTS WHO DIDN'T BOUGHT PROMOS (647 rows)
    * NON-PROMOTIONAL PURCHASES WITH SAME CLASIFICATION AS PROMOTIONAL ARTICLES (EXCLUDING CLIENTES WHO DIDN'T BOUGHT PROMOS) (424 rows)
    * PROMOTIONAL PURCHASES (2265 rows)
    * TOP 7 MOST PURCHASED ARTICLES
    * TOP 7 MOST PURCHASED PROMOTIONAL ARTICLES

As you can see in the next image, a view was created for each one of the listed points, followed by the queries used for each:

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/dbDiagram.png" width="455" height="742">

### CLIENTS WHO BOUGHT PROMOS (1290 rows)
```sh
        CREATE VIEW cliente_promo(id_cte)
        AS
        SELECT DISTINCT(c.id_cte)
        FROM compras c, promos p
        WHERE c.id_art=p.id_art;
```
<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/cliente_promo.png" width=“455” height=“742”>

### CLIENTS WHO DIDN'T BOUGHT PROMOS (647 rows)

```sh
        CREATE VIEW cliente_nopromo(id_cte)
        AS
        SELECT DISTINCT(id_cte)
        FROM compras
        WHERE id_cte
        NOT IN(
            SELECT DISTINCT(c.id_cte)
            FROM compras c, promos p
            WHERE c.id_art=p.id_art
        );
```

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/cliente_nopromo.png" width=“455” height=“742”>

### NON-PROMOTIONAL PURCHASES WITH SAME CLASIFICATION AS PROMOTIONAL ARTICLES (EXCLUDING CLIENTS WHO DIDN'T BOUGHT PROMOS) (424 rows)

```sh
        CREATE VIEW compras_no_promoxcte(id_cte, id_art, n_art)
        AS
        SELECT c.id_cte, c.id_art, COUNT(c.id_art)
        FROM compras c, catArt a
        WHERE c.id_art = a.id_art
        AND a.id_art
        NOT IN(
            SELECT DISTINCT(id_art)
            FROM promos
        ) AND a.id_clasif
        IN(
            SELECT DISTINCT(a.id_clasif)
            FROM catArt a, promos p
            WHERE a.id_art = p.id_art
        ) AND c.id_cte
        IN(
            SELECT DISTINCT(c.id_cte)
            FROM compras c, promos p
            WHERE c.id_art=p.id_art
        ) GROUP BY c.id_cte, c.id_art
        ORDER BY c.id_cte, COUNT(c.id_art) DESC;
```

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/compras_no_promoxcte.png" width=“455” height=“742”>

### PROMOTIONAL PURCHASES (2265 rows)

```sh
        CREATE VIEW compras_promoxcte(id_cte, id_art, n_art)
        AS
        SELECT c.id_cte, c.id_art, COUNT(c.id_art)
        FROM compras c, catArt a
        WHERE a.id_art = c.id_art
        AND a.id_art
        IN(
            SELECT DISTINCT(id_art)
            FROM promos
        ) AND c.id_cte
        IN(
            SELECT DISTINCT(c.id_cte)
            FROM compras c, promos p
            WHERE c.id_art=p.id_art
        ) GROUP BY c.id_cte, c.id_art
        ORDER BY c.id_cte, COUNT(c.id_art) DESC;
```

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/compras_promoxcte.png" width=“455” height=“742”>

### TOP 7 MOST PURCHASED ARTICLES

```sh
        CREATE VIEW TOP7T(id_art, n_art)
        AS
        SELECT id_art, COUNT(id_art)
        FROM compras
        GROUP BY id_art
        ORDER BY COUNT(id_art) DESC
        LIMIT 7;
```

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/TOP7T.png" width=“455” height=“742”>

### TOP 7 MOST PURCHASED PROMOTIONAL ARTICLES

```sh
        CREATE VIEW TOP7P(id_art, n_art)
        AS
        SELECT id_art, COUNT(id_art)
        FROM compras
        WHERE id_art
        IN(
            SELECT DISTINCT(id_art)
            FROM promos
        ) GROUP BY id_art
        ORDER BY COUNT(id_art) DESC
        LIMIT 7;
```

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/TOP7P.png" width=“455” height=“742”>

In order to be sure that all sets were complete and the views weren't missing data, I executed the following queries and analysis:

### NON-PROMOTIONAL PURCHASES WITH DISTINCT CLASIFICACION FROM PROMOTIONAL ARTICLES (EXCLUDING CLIENTS WHO DIDN'T BOUGHT PROMOS) (500 rows)

```sh
    SELECT c.id_cte, c.id_art, COUNT(c.id_art)
    FROM compras c, catArt a
    WHERE c.id_art = a.id_art
    AND a.id_art
    NOT IN(
        SELECT DISTINCT(id_art)
        FROM promos
    ) AND a.id_clasif
    NOT IN(
        SELECT DISTINCT(a.id_clasif)
        FROM catArt a, promos p
        WHERE a.id_art = p.id_art
    ) AND c.id_cte
    IN(
        SELECT DISTINCT(c.id_cte)
        FROM compras c, promos p
        WHERE c.id_art=p.id_art
    ) GROUP BY c.id_cte, c.id_art;
```

### TOTAL PURCHASES (EXCLUDING CLIENTS WHO DIDN'T BOUGHT PROMOS) (3189 rows)

```sh
    SELECT id_cte, id_art, COUNT(id_art)
    FROM compras
    WHERE id_cte
    IN(
        SELECT DISTINCT(c.id_cte)
        FROM compras c, promos p
        WHERE c.id_art=p.id_art
    ) GROUP BY id_cte, id_art;
```

### TOTAL PURCHASES (EXCLUDING CLIENTS WHO BOUGHT PROMOS) (647 rows)

```sh
    SELECT id_cte, id_art, COUNT(id_art)
    FROM compras
    WHERE id_cte
    NOT IN(
        SELECT DISTINCT(c.id_cte)
        FROM compras c, promos p
        WHERE c.id_art=p.id_art
    )
    GROUP BY id_cte, id_art;
```

### TOTAL PURCHASES WITH ALL CLIENTS (3836 rows)

```sh
    SELECT id_cte, id_art, COUNT(id_art)
    FROM compras
    GROUP BY id_cte, id_art;
```

### TOTAL CLIENTS (1937 rows)

```sh
    SELECT DISTINCT(id_cte) FROM compras;
```

Here a variable will be assigned to each set so we can picture the analysis clearly:
   
   * CLIENTS WHO BOUGHT PROMOS (1290 rows) => A
   * CLIENTS WHO DIDN'T BOUGHT PROMOS (647 rows) => B
   * TOTAL CLIENTS (1937 rows) => C
      * C = A + B
   * NON-PROMOTIONAL PURCHASES WITH SAME CLASIFICATION AS PROMOTIONAL ARTICLES (EXCLUDING CLIENTES WHO DIDN'T BOUGHT PROMOS) (424 rows) => A
   * NON-PROMOTIONAL PURCHASES WITH DISTINCT CLASIFICACION FROM PROMOTIONAL ARTICLES (EXCLUDING CLIENTS WHO DIDN’T BOUGHT PROMOS) (500 rows) => B
   * PROMOTIONAL PURCHASES (2265 rows) => C
   * TOTAL PURCHASES (EXCLUDING CLIENTS WHO DIDN’T BOUGHT PROMOS) (3189 rows) => D
      * D = A + B + C
   * TOTAL PURCHASES (EXCLUDING CLIENTS WHO BOUGHT PROMOS) (647 rows) => E
   * TOTAL PURCHASES WITH ALL CLIENTS (3836 rows) => F
      * F = D + E

As we can appreciate, all the sets complement each other, so we are not missing data from the created views.

Now that we have the required subsets for the analysis, we proceed to create the desired result set, for which I created
the following table, in which the result set will be inserted:
```sh
    CREATE TABLE prueba_tfinal
    (id_cte VARCHAR(10),
    id_art INT,
    n_art INT,
    PRIMARY KEY(id_cte, id_art));
```
I created a composed primary key, to be sure that the combination of client and article will not be repeated.
Finally, the procedures created to get the desired result set are:
*   insert_ctepromo_p
*   call_insert_cnp
*   call_insert_cp

I proceed to list them:
### insert_ctepromo_p
```sh
    delimiter //
    create procedure insert_ctepromo_p(
        IN id_entrada VARCHAR(10)
    )
    BEGIN
        DECLARE cte_count, cte_count2, count_cuenta, complemento, var_iter, i, dummy INT DEFAULT 0;
        SELECT COUNT(*) FROM compras_promoxcte WHERE id_cte = id_entrada INTO cte_count;
        INSERT INTO prueba_tfinal (id_cte, id_art, n_art)
        SELECT id_cte, id_art, n_art
        FROM compras_promoxcte
        WHERE id_cte = id_entrada
        LIMIT 7;
        
        IF cte_count < 8 THEN
            SELECT COUNT(*) FROM compras_no_promoxcte WHERE id_cte = id_entrada INTO cte_count2;
            SELECT cte_count + cte_count2 INTO count_cuenta;
    
            IF count_cuenta > 7 THEN
                SELECT 7 - cte_count INTO complemento;
                INSERT INTO prueba_tfinal (id_cte, id_art, n_art)
                SELECT id_cte, id_art, n_art
                FROM compras_no_promoxcte
                WHERE id_cte = id_entrada
                LIMIT 7;
            ELSEIF count_cuenta < 8 THEN
                INSERT INTO prueba_tfinal (id_cte, id_art, n_art)
                SELECT id_cte, id_art, n_art
                FROM compras_no_promoxcte
                WHERE id_cte = id_entrada
                LIMIT 7;
                SELECT 7 - count_cuenta INTO var_iter;
                IF var_iter > 0 THEN
                    WHILE i < var_iter DO
                        SELECT id_art
                        FROM TOP7T
                        WHERE id_art
                        NOT IN(
                            SELECT id_art
                            FROM prueba_tfinal
                            WHERE id_cte = id_entrada
                        )LIMIT 1 INTO dummy;
                        INSERT INTO prueba_tfinal (id_cte, id_art, n_art)
                        VALUES (id_entrada,
                        dummy, 1);
                        SELECT i + 1 INTO i;
                    END WHILE;
                END IF;
            END IF;
        END IF;
    end; //
```
### call_insert_cnp
```sh
    delimiter //
    create procedure p_call_insert_cnp()
    BEGIN
        DECLARE i, j, n INT DEFAULT 0;
        DECLARE dummy VARCHAR(10);
        SELECT COUNT(*) FROM cliente_nopromo INTO n;
        
        WHILE i<n DO 
            SELECT * FROM cliente_nopromo LIMIT i,1 INTO dummy;
            SET j = 0;
            WHILE j<7 DO
                INSERT INTO prueba_tfinal (id_cte, id_art, n_art)
                VALUES(dummy,
                (SELECT id_art FROM TOP7P LIMIT j,1),
                1);
                SELECT j + 1 INTO j;
            END WHILE;
            SELECT i + 1 INTO i;
        END WHILE;
    END; //
```
### call_insert_cp
```sh
    delimiter //
    create procedure p_call_insert_cp()
    BEGIN
        DECLARE i, n INT DEFAULT 0;
        DECLARE dummy VARCHAR(10);
        SELECT COUNT(*) FROM cliente_promo INTO n;
        
        WHILE i<n DO 
            SELECT * FROM cliente_promo LIMIT i,1 INTO dummy;
            CALL insert_ctepromo_p(dummy);
            SELECT i + 1 INTO i;
        END WHILE;
    END; //
```
<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/salida_1.png" width="186" height="963">
<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/salida_2.png" width="186" height="963">
<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/salida_3.png" width="186" height="963">

## Question 2

The steps for loading data from .csv into a database are:

* Create de database in the system.
* Stablish a connection with some vendor of database with it's connector (in this case I used MySql).
* With the connection object of the last step create a connector object and select the database in order to excecute queries over the data base.
* Use the connector object to create the tables into the database passing as arguments the "create table" sql sentence (notice that I have created the relationships between the tables).
* In this step I load all the data from the .csv files into the tables that I have created in the last step, running the "INSER INTO" sql setence into a for loop for each table.
* As last step I put all together in python functions and to prove that the data was created I execute a "SELECT *" sql sentence over the three tables.
    
Finally I attach the code with some documentation and some images:

You can see te ETL functions in the next section:

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

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/punto_2.png" width="1920" height="1080">

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
            
            dataFrameOutset = pd.merge(dataFrameCarArt, dataFrameCompras, on='id_art', how='inner')
           
            dataFrameOutset.to_csv('{YourPath}')
    except Error as e:
        print("Error while connecting to MySQL", e)
    
datasetsToSqlToCsv()
```
<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/punto_3_1.png" width="1920" height="1080">

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/punto_3_2.png" width="1920" height="1080">

<img src="https://github.com/alegorriz/examenTecnico/blob/main/image/punto_3_3.png" width="1920" height="1080">
