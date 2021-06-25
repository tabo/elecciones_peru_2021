CREATE TABLE presidencial AS
SELECT v2a.mesa,

       v2a.TOT_CIUDADANOS_VOTARON AS 'v2_TOT_CIUDADANOS_VOTARON',
       v2_fp.congresal            AS 'v2_fp',
       v2_perulibre.congresal     AS 'v2_perulibre',
       v2_emitidos.congresal      AS 'v2_emitidos',
       v2_validos.congresal       AS 'v2_validos',
       v2_blanco.congresal        AS 'v2_blanco',
       v2_impugnados.congresal    AS 'v2_impugnados',
       v2_nulos.congresal         AS 'v2_nulos',

       v2a.OBSERVACION            AS 'v2_OBSERVACION',
       v2a.OBSERVACION_TXT        AS 'v2_OBSERVACION_TXT',

       v1a.TOT_CIUDADANOS_VOTARON AS 'v1_TOT_CIUDADANOS_VOTARON',
       v1_fp.congresal            AS 'v1_fp',
       v1_perulibre.congresal     AS 'v1_perulibre',
       v1_emitidos.congresal      AS 'v1_emitidos',
       v1_validos.congresal       AS 'v1_validos',
       v1_blanco.congresal        AS 'v1_blanco',
       v1_impugnados.congresal    AS 'v1_impugnados',
       v1_nulos.congresal         AS 'v1_nulos',
       v1_ap.congresal            AS 'v1_ap',
       v1_app.congresal           AS 'v1_app',
       v1_avpais.congresal        AS 'v1_avpais',
       v1_dd.congresal            AS 'v1_dd',
       v1_fa.congresal            AS 'v1_fa',
       v1_jpp.congresal           AS 'v1_jpp',
       v1_sp.congresal            AS 'v1_sp',
       v1_morado.congresal        AS 'v1_morado',
       v1_pnp.congresal           AS 'v1_pnp',
       v1_ppc.congresal           AS 'v1_ppc',
       v1_pps.congresal           AS 'v1_pps',
       v1_pp.congresal            AS 'v1_pp',
       v1_runa.congresal          AS 'v1_runa',
       v1_rp.congresal            AS 'v1_rp',
       v1_upp.congresal           AS 'v1_upp',
       v1_vn.congresal            AS 'v1_vn',

       v1a.OBSERVACION            AS 'v1_OBSERVACION',
       v1a.OBSERVACION_TXT        AS 'v1_OBSERVACION_TXT',

       v2a.CCODI_UBIGEO           AS 'v2_CCODI_UBIGEO',
       v2a.DEPARTAMENTO           AS 'v2_DEPARTAMENTO',
       v2a.PROVINCIA              AS 'v2_PROVINCIA',
       v2a.DISTRITO               AS 'v2_DISTRITO',
       v2a.TDIRE_LOCAL            AS 'v2_TDIRE_LOCAL',
       v2a.TNOMB_LOCAL            AS 'v2_TNOMB_LOCAL',
       v2a.CCOPIA_ACTA            AS 'v2_CCOPIA_ACTA',
       v2a.NNUME_HABILM           AS 'v2_NNUME_HABILM',
       v2a.N_CANDIDATOS           AS 'v2_N_CANDIDATOS',
       v2a.CCENT_COMPU            AS 'v2_CCENT_COMPU',

       v1a.CCODI_UBIGEO           AS 'v1_CCODI_UBIGEO',
       v1a.DEPARTAMENTO           AS 'v1_DEPARTAMENTO',
       v1a.PROVINCIA              AS 'v1_PROVINCIA',
       v1a.DISTRITO               AS 'v1_DISTRITO',
       v1a.TDIRE_LOCAL            AS 'v1_TDIRE_LOCAL',
       v1a.TNOMB_LOCAL            AS 'v1_TNOMB_LOCAL',
       v1a.CCOPIA_ACTA            AS 'v1_CCOPIA_ACTA',
       v1a.NNUME_HABILM           AS 'v1_NNUME_HABILM',
       v1a.N_CANDIDATOS           AS 'v1_N_CANDIDATOS',
       v1a.CCENT_COMPU            AS 'v1_CCENT_COMPU'

FROM actas_20210606 AS v2a
         LEFT JOIN actas_20210411 AS v1a ON v1a.mesa = v2a.mesa AND v1a.tipo_acta = v2a.tipo_acta
         LEFT JOIN votos_20210411 AS v1_ap
                   ON v2a.mesa = v1_ap.mesa AND v2a.tipo_acta = v1_ap.tipo_acta AND v1_ap.AUTORIDAD = 'ACCION POPULAR'
         LEFT JOIN votos_20210411 AS v1_app ON v2a.mesa = v1_app.mesa AND v2a.tipo_acta = v1_app.tipo_acta AND
                                               v1_app.AUTORIDAD = 'ALIANZA PARA EL PROGRESO'
         LEFT JOIN votos_20210411 AS v1_avpais ON v2a.mesa = v1_avpais.mesa AND v2a.tipo_acta = v1_avpais.tipo_acta AND
                                                  v1_avpais.AUTORIDAD = 'AVANZA PAIS - PARTIDO DE INTEGRACION SOCIAL'
         LEFT JOIN votos_20210411 AS v1_dd ON v2a.mesa = v1_dd.mesa AND v2a.tipo_acta = v1_dd.tipo_acta AND
                                              v1_dd.AUTORIDAD = 'DEMOCRACIA DIRECTA'
         LEFT JOIN votos_20210411 AS v1_fa ON v2a.mesa = v1_fa.mesa AND v2a.tipo_acta = v1_fa.tipo_acta AND
                                              v1_fa.AUTORIDAD = 'EL FRENTE AMPLIO POR JUSTICIA, VIDA Y LIBERTAD'
         LEFT JOIN votos_20210411 AS v1_fp
                   ON v2a.mesa = v1_fp.mesa AND v2a.tipo_acta = v1_fp.tipo_acta AND v1_fp.AUTORIDAD = 'FUERZA POPULAR'
         LEFT JOIN votos_20210411 AS v1_jpp ON v2a.mesa = v1_jpp.mesa AND v2a.tipo_acta = v1_jpp.tipo_acta AND
                                               v1_jpp.AUTORIDAD = 'JUNTOS POR EL PERU'
         LEFT JOIN votos_20210411 AS v1_sp ON v2a.mesa = v1_sp.mesa AND v2a.tipo_acta = v1_sp.tipo_acta AND
                                              v1_sp.AUTORIDAD = 'PARTIDO DEMOCRATICO SOMOS PERU'
         LEFT JOIN votos_20210411 AS v1_morado ON v2a.mesa = v1_morado.mesa AND v2a.tipo_acta = v1_morado.tipo_acta AND
                                                  v1_morado.AUTORIDAD = 'PARTIDO MORADO'
         LEFT JOIN votos_20210411 AS v1_pnp ON v2a.mesa = v1_pnp.mesa AND v2a.tipo_acta = v1_pnp.tipo_acta AND
                                               v1_pnp.AUTORIDAD = 'PARTIDO NACIONALISTA PERUANO'
         LEFT JOIN votos_20210411 AS v1_perulibre
                   ON v2a.mesa = v1_perulibre.mesa AND v2a.tipo_acta = v1_perulibre.tipo_acta AND
                      v1_perulibre.AUTORIDAD = 'PARTIDO POLITICO NACIONAL PERU LIBRE'
         LEFT JOIN votos_20210411 AS v1_ppc ON v2a.mesa = v1_ppc.mesa AND v2a.tipo_acta = v1_ppc.tipo_acta AND
                                               v1_ppc.AUTORIDAD = 'PARTIDO POPULAR CRISTIANO - PPC'
         LEFT JOIN votos_20210411 AS v1_pps ON v2a.mesa = v1_pps.mesa AND v2a.tipo_acta = v1_pps.tipo_acta AND
                                               v1_pps.AUTORIDAD = 'PERU PATRIA SEGURA'
         LEFT JOIN votos_20210411 AS v1_pp
                   ON v2a.mesa = v1_pp.mesa AND v2a.tipo_acta = v1_pp.tipo_acta AND v1_pp.AUTORIDAD = 'PODEMOS PERU'
         LEFT JOIN votos_20210411 AS v1_runa ON v2a.mesa = v1_runa.mesa AND v2a.tipo_acta = v1_runa.tipo_acta AND
                                                v1_runa.AUTORIDAD = 'RENACIMIENTO UNIDO NACIONAL'
         LEFT JOIN votos_20210411 AS v1_rp ON v2a.mesa = v1_rp.mesa AND v2a.tipo_acta = v1_rp.tipo_acta AND
                                              v1_rp.AUTORIDAD = 'RENOVACION POPULAR'
         LEFT JOIN votos_20210411 AS v1_emitidos
                   ON v2a.mesa = v1_emitidos.mesa AND v2a.tipo_acta = v1_emitidos.tipo_acta AND
                      v1_emitidos.AUTORIDAD = 'TOTAL VOTOS EMITIDOS'
         LEFT JOIN votos_20210411 AS v1_validos
                   ON v2a.mesa = v1_validos.mesa AND v2a.tipo_acta = v1_validos.tipo_acta AND
                      v1_validos.AUTORIDAD = 'TOTAL VOTOS VALIDOS'
         LEFT JOIN votos_20210411 AS v1_upp ON v2a.mesa = v1_upp.mesa AND v2a.tipo_acta = v1_upp.tipo_acta AND
                                               v1_upp.AUTORIDAD = 'UNION POR EL PERU'
         LEFT JOIN votos_20210411 AS v1_vn ON v2a.mesa = v1_vn.mesa AND v2a.tipo_acta = v1_vn.tipo_acta AND
                                              v1_vn.AUTORIDAD = 'VICTORIA NACIONAL'
         LEFT JOIN votos_20210411 AS v1_blanco ON v2a.mesa = v1_blanco.mesa AND v2a.tipo_acta = v1_blanco.tipo_acta AND
                                                  v1_blanco.AUTORIDAD = 'VOTOS EN BLANCO'
         LEFT JOIN votos_20210411 AS v1_impugnados
                   ON v2a.mesa = v1_impugnados.mesa AND v2a.tipo_acta = v1_impugnados.tipo_acta AND
                      v1_impugnados.AUTORIDAD = 'VOTOS IMPUGNADOS'
         LEFT JOIN votos_20210411 AS v1_nulos ON v2a.mesa = v1_nulos.mesa AND v2a.tipo_acta = v1_nulos.tipo_acta AND
                                                 v1_nulos.AUTORIDAD = 'VOTOS NULOS'
         LEFT JOIN votos_20210606 AS v2_fp
                   ON v2a.mesa = v2_fp.mesa AND v2a.tipo_acta = v2_fp.tipo_acta AND v2_fp.AUTORIDAD = 'FUERZA POPULAR'
         LEFT JOIN votos_20210606 AS v2_perulibre
                   ON v2a.mesa = v2_perulibre.mesa AND v2a.tipo_acta = v2_perulibre.tipo_acta AND
                      v2_perulibre.AUTORIDAD = 'PARTIDO POLITICO NACIONAL PERU LIBRE'
         LEFT JOIN votos_20210606 AS v2_emitidos
                   ON v2a.mesa = v2_emitidos.mesa AND v2a.tipo_acta = v2_emitidos.tipo_acta AND
                      v2_emitidos.AUTORIDAD = 'TOTAL VOTOS EMITIDOS'
         LEFT JOIN votos_20210606 AS v2_validos
                   ON v2a.mesa = v2_validos.mesa AND v2a.tipo_acta = v2_validos.tipo_acta AND
                      v2_validos.AUTORIDAD = 'TOTAL VOTOS VALIDOS'
         LEFT JOIN votos_20210606 AS v2_blanco ON v2a.mesa = v2_blanco.mesa AND v2a.tipo_acta = v2_blanco.tipo_acta AND
                                                  v2_blanco.AUTORIDAD = 'VOTOS EN BLANCO'
         LEFT JOIN votos_20210606 AS v2_impugnados
                   ON v2a.mesa = v2_impugnados.mesa AND v2a.tipo_acta = v2_impugnados.tipo_acta AND
                      v2_impugnados.AUTORIDAD = 'VOTOS IMPUGNADOS'
         LEFT JOIN votos_20210606 AS v2_nulos ON v2a.mesa = v2_nulos.mesa AND v2a.tipo_acta = v2_nulos.tipo_acta AND
                                                 v2_nulos.AUTORIDAD = 'VOTOS NULOS'
WHERE v2a.tipo_acta = 'presidencial'
;