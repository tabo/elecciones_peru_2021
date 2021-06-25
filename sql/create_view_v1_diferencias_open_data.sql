CREATE VIEW v1_diferencias_open_data
AS
SELECT o.MESA_DE_VOTACION,
       o.UBIGEO,
       r.v1_CCODI_UBIGEO,
       o.DEPARTAMENTO,
       r.v1_DEPARTAMENTO,
       o.PROVINCIA,
       r.v1_PROVINCIA,
       o.DISTRITO,
       r.v1_DISTRITO,
       o.TIPO_ELECCION,
       'PRESIDENCIAL'                                            AS v1_tipo_eleccion,
       o.DESCRIP_ESTADO_ACTA,
       r.v1_OBSERVACION,
       r.v1_OBSERVACION_TXT,
       o.TIPO_OBSERVACION,
       (CASE WHEN o.N_CVAS <> '' THEN o.N_CVAS ELSE 0 END)       AS N_CVAS,
       r.v1_TOT_CIUDADANOS_VOTARON,
       o.N_ELEC_HABIL,
       r.v1_NNUME_HABILM,
       (CASE WHEN o.VOTOS_P1 <> '' THEN o.VOTOS_P1 ELSE 0 END)   AS VOTOS_P1,
       r.v1_pnp,
       (CASE WHEN o.VOTOS_P2 <> '' THEN o.VOTOS_P2 ELSE 0 END)   AS VOTOS_P2,
       r.v1_fa,
       (CASE WHEN o.VOTOS_P3 <> '' THEN o.VOTOS_P3 ELSE 0 END)   AS VOTOS_P3,
       r.v1_morado,
       (CASE WHEN o.VOTOS_P4 <> '' THEN o.VOTOS_P4 ELSE 0 END)   AS VOTOS_P4,
       r.v1_pps,
       (CASE WHEN o.VOTOS_P5 <> '' THEN o.VOTOS_P5 ELSE 0 END)   AS VOTOS_P5,
       r.v1_vn,
       (CASE WHEN o.VOTOS_P6 <> '' THEN o.VOTOS_P6 ELSE 0 END)   AS VOTOS_P6,
       r.v1_ap,
       (CASE WHEN o.VOTOS_P7 <> '' THEN o.VOTOS_P7 ELSE 0 END)   AS VOTOS_P7,
       r.v1_avpais,
       (CASE WHEN o.VOTOS_P8 <> '' THEN o.VOTOS_P8 ELSE 0 END)   AS VOTOS_P8,
       r.v1_pp,
       (CASE WHEN o.VOTOS_P9 <> '' THEN o.VOTOS_P9 ELSE 0 END)   AS VOTOS_P9,
       r.v1_jpp,
       (CASE WHEN o.VOTOS_P10 <> '' THEN o.VOTOS_P10 ELSE 0 END) AS VOTOS_P10,
       r.v1_ppc,
       (CASE WHEN o.VOTOS_P11 <> '' THEN o.VOTOS_P11 ELSE 0 END) AS VOTOS_P11,
       r.v1_fp,
       (CASE WHEN o.VOTOS_P12 <> '' THEN o.VOTOS_P12 ELSE 0 END) AS VOTOS_P12,
       r.v1_upp,
       (CASE WHEN o.VOTOS_P13 <> '' THEN o.VOTOS_P13 ELSE 0 END) AS VOTOS_P13,
       r.v1_rp,
       (CASE WHEN o.VOTOS_P14 <> '' THEN o.VOTOS_P14 ELSE 0 END) AS VOTOS_P14,
       r.v1_runa,
       (CASE WHEN o.VOTOS_P15 <> '' THEN o.VOTOS_P15 ELSE 0 END) AS VOTOS_P15,
       r.v1_sp,
       (CASE WHEN o.VOTOS_P16 <> '' THEN o.VOTOS_P16 ELSE 0 END) AS VOTOS_P16,
       r.v1_perulibre,
       (CASE WHEN o.VOTOS_P17 <> '' THEN o.VOTOS_P17 ELSE 0 END) AS VOTOS_P17,
       r.v1_dd,
       (CASE WHEN o.VOTOS_P18 <> '' THEN o.VOTOS_P18 ELSE 0 END) AS VOTOS_P18,
       r.v1_app,
       (CASE WHEN o.VOTOS_VB <> '' THEN o.VOTOS_VB ELSE 0 END)   AS VOTOS_VB,
       r.v1_blanco,
       (CASE WHEN o.VOTOS_VN <> '' THEN o.VOTOS_VN ELSE 0 END)   AS VOTOS_VN,
       r.v1_nulos,
       (CASE WHEN o.VOTOS_VI <> '' THEN o.VOTOS_VI ELSE 0 END)   AS VOTOS_VI,
       r.v1_impugnados
FROM onpe_pcm_v1 AS o
         INNER JOIN presidencial AS r
                    ON o.MESA_DE_VOTACION = r.mesa
WHERE o.UBIGEO <> r.v1_CCODI_UBIGEO
   --    OR o.DEPARTAMENTO <> r.v1_DEPARTAMENTO
   --    OR o.PROVINCIA <> r.v1_PROVINCIA
   --    OR o.DISTRITO <> r.v1_DISTRITO
   OR o.TIPO_ELECCION <> 'PRESIDENCIAL'
   OR (
        o.DESCRIP_ESTADO_ACTA == 'CONTABILIZADA'
        AND (
                r.v1_OBSERVACION <> 'CONTABILIZADAS NORMALES'
                OR v1_OBSERVACION_TXT <> 'ACTA ELECTORAL NORMAL'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'COMPUTADA RESUELTA'
        AND (
                r.v1_OBSERVACION <> 'CONTABILIZADAS NORMALES'
                OR v1_OBSERVACION_TXT <> 'ACTA ELECTORAL RESUELTA'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'ANULADA'
        AND (
                r.v1_OBSERVACION <> 'ACTA ELECTORAL ANULADA'
                OR v1_OBSERVACION_TXT <> 'ACTA ELECTORAL RESUELTA'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'SIN INSTALAR'
        AND (
                r.v1_OBSERVACION <> 'MESA NO INSTALADA'
                OR v1_OBSERVACION_TXT <> 'MESA NO INSTALADA'
            )
    )
   OR (
        o.DESCRIP_ESTADO_ACTA == 'EN PROCESO'
        AND r.v1_OBSERVACION IN ('ACTA ELECTORAL ANULADA', 'CONTABILIZADAS NORMALES', 'MESA NO INSTALADA')
    )
   OR (CASE WHEN o.N_CVAS <> '' THEN o.N_CVAS ELSE 0 END) <> r.v1_TOT_CIUDADANOS_VOTARON
   OR o.N_ELEC_HABIL <> r.v1_NNUME_HABILM
   OR (CASE WHEN o.VOTOS_P1 <> '' THEN o.VOTOS_P1 ELSE 0 END) <> r.v1_pnp
   OR (CASE WHEN o.VOTOS_P2 <> '' THEN o.VOTOS_P2 ELSE 0 END) <> r.v1_fa
   OR (CASE WHEN o.VOTOS_P3 <> '' THEN o.VOTOS_P3 ELSE 0 END) <> r.v1_morado
   OR (CASE WHEN o.VOTOS_P4 <> '' THEN o.VOTOS_P4 ELSE 0 END) <> r.v1_pps
   OR (CASE WHEN o.VOTOS_P5 <> '' THEN o.VOTOS_P5 ELSE 0 END) <> r.v1_vn
   OR (CASE WHEN o.VOTOS_P6 <> '' THEN o.VOTOS_P6 ELSE 0 END) <> r.v1_ap
   OR (CASE WHEN o.VOTOS_P7 <> '' THEN o.VOTOS_P7 ELSE 0 END) <> r.v1_avpais
   OR (CASE WHEN o.VOTOS_P8 <> '' THEN o.VOTOS_P8 ELSE 0 END) <> r.v1_pp
   OR (CASE WHEN o.VOTOS_P9 <> '' THEN o.VOTOS_P9 ELSE 0 END) <> r.v1_jpp
   OR (CASE WHEN o.VOTOS_P10 <> '' THEN o.VOTOS_P10 ELSE 0 END) <> r.v1_ppc
   OR (CASE WHEN o.VOTOS_P11 <> '' THEN o.VOTOS_P11 ELSE 0 END) <> r.v1_fp
   OR (CASE WHEN o.VOTOS_P12 <> '' THEN o.VOTOS_P12 ELSE 0 END) <> r.v1_upp
   OR (CASE WHEN o.VOTOS_P13 <> '' THEN o.VOTOS_P13 ELSE 0 END) <> r.v1_rp
   OR (CASE WHEN o.VOTOS_P14 <> '' THEN o.VOTOS_P14 ELSE 0 END) <> r.v1_runa
   OR (CASE WHEN o.VOTOS_P15 <> '' THEN o.VOTOS_P15 ELSE 0 END) <> r.v1_sp
   OR (CASE WHEN o.VOTOS_P16 <> '' THEN o.VOTOS_P16 ELSE 0 END) <> r.v1_perulibre
   OR (CASE WHEN o.VOTOS_P17 <> '' THEN o.VOTOS_P17 ELSE 0 END) <> r.v1_dd
   OR (CASE WHEN o.VOTOS_P18 <> '' THEN o.VOTOS_P18 ELSE 0 END) <> r.v1_app
   OR (CASE WHEN o.VOTOS_VB <> '' THEN o.VOTOS_VB ELSE 0 END) <> r.v1_blanco
   OR (CASE WHEN o.VOTOS_VN <> '' THEN o.VOTOS_VN ELSE 0 END) <> r.v1_nulos
   OR (CASE WHEN o.VOTOS_VI <> '' THEN o.VOTOS_VI ELSE 0 END) <> r.v1_impugnados;