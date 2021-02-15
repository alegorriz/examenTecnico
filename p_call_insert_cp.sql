drop procedure p_call_insert_cp;
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

delimiter ;

call p_call_insert_cp();


