drop procedure insert_ctepromo_p;
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

delimiter ;

truncate prueba_tfinal;
