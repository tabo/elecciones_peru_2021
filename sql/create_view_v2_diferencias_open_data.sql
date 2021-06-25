CREATE VIEW v2_diferencias_open_data
AS
SELECT o.MESA_DE_VOTACION,
       o.UBIGEO,
       r.v2_CCODI_UBIGEO,
       o.DEPARTAMENTO,
       r.v2_DEPARTAMENTO,
       o.PROVINCIA,
       r.v2_PROVINCIA,
       o.DISTRITO,
       r.v2_DISTRITO,
       o.TIPO_ELECCION,
       'PRESIDENCIAL'                                          AS v2_tipo_eleccion,
       o.DESCRIP_ESTADO_ACTA,
       r.v2_OBSERVACION,
       r.v2_OBSERVACION_TXT,
       o.TIPO_OBSERVACION,
       (CASE WHEN o.N_CVAS <> '' THEN o.N_CVAS ELSE 0 END)     AS N_CVAS,
       r.v2_TOT_CIUDADANOS_VOTARON,
       o.N_ELEC_HABIL,
       r.v2_NNUME_HABILM,
       (CASE WHEN o.VOTOS_P1 <> '' THEN o.VOTOS_P1 ELSE 0 END) AS VOTOS_P1,
       r.v2_perulibre,
       (CASE WHEN o.VOTOS_P2 <> '' THEN o.VOTOS_P2 ELSE 0 END) AS VOTOS_P2,
       r.v2_fp,
       (CASE WHEN o.VOTOS_VB <> '' THEN o.VOTOS_VB ELSE 0 END) AS VOTOS_VB,
       r.v2_blanco,
       (CASE WHEN o.VOTOS_VN <> '' THEN o.VOTOS_VN ELSE 0 END) AS VOTOS_VN,
       r.v2_nulos,
       (CASE WHEN o.VOTOS_VI <> '' THEN o.VOTOS_VI ELSE 0 END) AS VOTOS_VI,
       r.v2_impugnados
FROM onpe_pcm_v2 AS o
         LEFT JOIN presidencial AS r
                   ON o.MESA_DE_VOTACION = r.mesa
WHERE o.UBIGEO <> r.v2_CCODI_UBIGEO
   OR o.DEPARTAMENTO <> r.v2_DEPARTAMENTO
   -- OR o.PROVINCIA <> r.v2_PROVINCIA
   -- OR o.DISTRITO <> r.v2_DISTRITO
   OR o.TIPO_ELECCION <> 'PRESIDENCIAL'
   OR (
        o.DESCRIP_ESTADO_ACTA == 'CONTABILIZADA'
        AND (
                r.v2_OBSERVACION <> 'CONTABILIZADAS NORMALES'
                OR v2_OBSERVACION_TXT <> 'ACTA ELECTORAL NORMAL'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'COMPUTADA RESUELTA'
        AND (
                r.v2_OBSERVACION <> 'CONTABILIZADAS NORMALES'
                OR v2_OBSERVACION_TXT <> 'ACTA ELECTORAL RESUELTA'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'ANULADA'
        AND (
                r.v2_OBSERVACION <> 'ACTA ELECTORAL ANULADA'
                OR v2_OBSERVACION_TXT <> 'ACTA ELECTORAL RESUELTA'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'SIN INSTALAR'
        AND (
                r.v2_OBSERVACION <> 'MESA NO INSTALADA'
                OR v2_OBSERVACION_TXT <> 'MESA NO INSTALADA'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'EN PROCESO'
        AND r.v2_OBSERVACION IN ('ACTA ELECTORAL ANULADA', 'CONTABILIZADAS NORMALES', 'MESA NO INSTALADA')
    )
   OR (CASE WHEN o.N_CVAS <> '' THEN o.N_CVAS ELSE 0 END) <> r.v2_TOT_CIUDADANOS_VOTARON
   OR o.N_ELEC_HABIL <> r.v2_NNUME_HABILM
   OR (CASE WHEN o.VOTOS_P1 <> '' THEN o.VOTOS_P1 ELSE 0 END) <> r.v2_perulibre
   OR (CASE WHEN o.VOTOS_P2 <> '' THEN o.VOTOS_P2 ELSE 0 END) <> r.v2_fp
   OR (CASE WHEN o.VOTOS_VB <> '' THEN o.VOTOS_VB ELSE 0 END) <> r.v2_blanco
   OR (CASE WHEN o.VOTOS_VN <> '' THEN o.VOTOS_VN ELSE 0 END) <> r.v2_nulos
   OR (CASE WHEN o.VOTOS_VI <> '' THEN o.VOTOS_VI ELSE 0 END) <> r.v2_impugnados