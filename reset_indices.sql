select * from ctrl_dispersion
SELECT * FROM information_schema.sequences WHERE sequence_name LIKE 'dispersion%';
ALTER SEQUENCE ctrl_dispersion_ctrl_id_seq RESTART WITH 1;
ALTER SEQUENCE dispersion_id_seq	 RESTART WITH 1;
