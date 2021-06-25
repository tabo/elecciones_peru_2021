CREATE VIEW payasada_summary
AS
SELECT v2_DEPARTAMENTO,
       SUM(hallazgos_perulibre_payaso)      AS hallazgos_perulibre_payaso,
       SUM(hallazgos_perulibre_corregido)   AS hallazgos_perulibre_corregido,
       SUM(sospechosos_perulibre_payaso)    AS sospechosos_perulibre_payaso,
       SUM(sospechosos_perulibre_corregido) AS sospechosos_perulibre_corregido,
       SUM(hallazgos_fp_payaso)             AS hallazgos_fp_payaso,
       SUM(hallazgos_fp_corregido)          AS hallazgos_fp_corregido,
       SUM(sospechosos_fp_payaso)           AS sospechosos_fp_payaso,
       SUM(sospechosos_fp_corregido)        AS sospechosos_fp_corregido
FROM (
         SELECT v2_DEPARTAMENTO,
                COUNT(1) AS hallazgos_perulibre_payaso,
                0        AS hallazgos_perulibre_corregido,
                0        AS sospechosos_perulibre_payaso,
                0        AS sospechosos_perulibre_corregido,
                0        AS hallazgos_fp_payaso,
                0        AS hallazgos_fp_corregido,
                0        AS sospechosos_fp_payaso,
                0        AS sospechosos_fp_corregido
         FROM payasada
         WHERE hallazgo_perulibre_payaso
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                COUNT(1),
                0,
                0,
                0,
                0,
                0,
                0
         FROM payasada
         WHERE hallazgo_perulibre_corregido
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                0,
                COUNT(1),
                0,
                0,
                0,
                0,
                0
         FROM payasada
         WHERE sospechoso_perulibre_payaso
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                0,
                0,
                COUNT(1),
                0,
                0,
                0,
                0
         FROM payasada
         WHERE sospechoso_perulibre_corregido
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                0,
                0,
                0,
                COUNT(1),
                0,
                0,
                0
         FROM payasada
         WHERE hallazgo_fp_payaso
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                0,
                0,
                0,
                0,
                COUNT(1),
                0,
                0
         FROM payasada
         WHERE hallazgo_fp_corregido
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                0,
                0,
                0,
                0,
                0,
                COUNT(1),
                0
         FROM payasada
         WHERE sospechoso_fp_payaso
         GROUP BY v2_DEPARTAMENTO
         UNION
         SELECT v2_DEPARTAMENTO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                COUNT(1)
         FROM payasada
         WHERE sospechoso_fp_corregido
         GROUP BY v2_DEPARTAMENTO
     )
GROUP BY v2_DEPARTAMENTO
;