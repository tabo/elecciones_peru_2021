CREATE VIEW v1_mesas_faltantes
AS
SELECT *
FROM onpe_pcm_v1
WHERE MESA_DE_VOTACION NOT IN (
    SELECT mesa
    FROM presidencial
    WHERE v1_CCODI_UBIGEO IS NOT NULL
);