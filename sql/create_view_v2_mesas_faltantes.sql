CREATE VIEW v2_mesas_faltantes
AS
SELECT *
FROM onpe_pcm_v2
WHERE MESA_DE_VOTACION NOT IN (
    SELECT mesa
    FROM presidencial
    WHERE v2_CCODI_UBIGEO IS NOT NULL
);