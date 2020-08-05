CREATE OR REPLACE PROCEDURE cleanup_dupes()
LANGUAGE 'plpgsql'
AS $$
DECLARE
    c_before bigint;
	c_after bigint;
BEGIN
	RAISE NOTICE 'Rows before cleanup %.', (SELECT COUNT(*) FROM atoms_b);
    c_before = (SELECT COUNT(DISTINCT data_json) FROM atoms_b);
	
	-- Delete dupes
	DELETE FROM
    atoms_b a USING atoms_b b
	WHERE a.id < b.id
    AND a.data_json = b.data_json;
	
	c_after = (SELECT COUNT(DISTINCT data_json) FROM atoms_b);
	-- If some meaningful data was deleted, rollback.
	IF c_after < c_before THEN
		RAISE WARNING 'Some meaningful data got deleted, rolling back.';
		ROLLBACK;
		RETURN;
	ELSE
		COMMIT;
	END IF;
	
	RAISE NOTICE 'Rows after cleanup %.', (SELECT COUNT(*) FROM atoms_b);
END;
$$;
