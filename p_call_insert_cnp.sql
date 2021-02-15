drop procedure p_call_insert_cnp;
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

delimiter ;

call p_call_insert_cnp();
