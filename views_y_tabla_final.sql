--SUBCONJUNTO CLIENTES PROMO (1290):
DROP VIEW cliente_promo;
CREATE VIEW cliente_promo(id_cte)
AS
SELECT DISTINCT(c.id_cte)
FROM compras c, promos p
WHERE c.id_art=p.id_art;

--SUBCONJUNTO CLIENTES SIN PROMO (647):
DROP VIEW cliente_nopromo;
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

--COMPRAS NO PROMOCIONALES
--(PERO CON MISMA CLASIFICACION QUE ARTICULOS PROMOCIONALES)
--POR CLIENTE (EXCLUYENDO A LOS CLIENTES QUE NO COMPRARON PROMOCIONES)(424)

DROP VIEW compras_no_promoxcte;
CREATE VIEW compras_no_promoxcte(id_cte, id_art, n_art)
AS
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
) AND c.id_cte
IN(
    SELECT DISTINCT(c.id_cte)
    FROM compras c, promos p
    WHERE c.id_art=p.id_art
) GROUP BY c.id_cte, c.id_art
ORDER BY c.id_cte, COUNT(c.id_art) DESC;

--COMPRAS PROMOCIONALES POR CLIENTE (2265)

DROP VIEW compras_promoxcte;
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

-- TOP 7 COMPRAS (303) [TOP7T]

DROP VIEW TOP7T;
CREATE VIEW TOP7T(id_art, n_art)
AS
SELECT id_art, COUNT(id_art)
FROM compras
GROUP BY id_art
ORDER BY COUNT(id_art) DESC
LIMIT 7;

--TOP 7 PROMOCIONES (178) [TOP7P]

DROP VIEW TOP7P;
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

--CREACION DE TABLA SOLUCION

CREATE TABLE prueba_tfinal
(id_cte VARCHAR(10),
id_art INT,
n_art INT,
PRIMARY KEY(id_cte, id_art));

SELECT id_cte, id_art from prueba_tfinal
WHERE id_cte
IN(
    SELECT DISTINCT(c.id_cte)
    FROM compras c, promos p
    WHERE c.id_art=p.id_art
);