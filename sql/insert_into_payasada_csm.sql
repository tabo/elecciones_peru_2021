INSERT INTO payasada_reloaded
SELECT *,
       (NOT anulada AND acta_falsa_perulibre = 'Hallazgo')   AS hallazgo_perulibre,
       (NOT anulada AND acta_falsa_perulibre = 'Sospechoso') AS sospechoso_perulibre,
       (NOT anulada AND acta_falsa_fp = 'Hallazgo')          AS hallazgo_fp,
       (NOT anulada AND acta_falsa_fp = 'Sospechoso')        AS sospechoso_fp
FROM (
         SELECT *,
                (acta_falsa_perulibre = 'Hallazgo' AND acta_falsa_fp = 'Hallazgo') AS anulada

         FROM (
                  SELECT *,
                         (
                             CASE
                                 WHEN esc3_prob_perulibre < 0.001 THEN 'Imposible'
                                 WHEN esc3_prob_perulibre < 0.01 THEN 'Improbable'
                                 WHEN esc3_prob_perulibre < 0.1 THEN 'Muy Poco Probable'
                                 ELSE ''
                                 END
                             ) AS esc3_desc_perulibre,
                         (
                             CASE
                                 WHEN esc3_prob_fp < 0.001 THEN 'Imposible'
                                 WHEN esc3_prob_fp < 0.01 THEN 'Improbable'
                                 WHEN esc3_prob_fp < 0.1 THEN 'Muy Poco Probable'
                                 ELSE ''
                                 END
                             ) AS esc3_desc_fp,
                         (
                             CASE
                                 WHEN v2_blanco = 0 THEN 'Alerta por Blancos'
                                 WHEN v2_nulos = 0 THEN 'Alerta por Nulos'
                                 ELSE ''
                                 END
                             ) AS alerta_por_nulos_y_blancos,
                         (
                             CASE
                                 WHEN esc3_prob_perulibre < 0.1 THEN (
                                     CASE
                                         WHEN NOT cumplimiento_esc2_perulibre
                                             THEN 'Hallazgo'
                                         WHEN NOT cumplimiento_esc1_perulibre
                                             THEN 'Sospechoso'
                                         ELSE ''
                                         END
                                     )
                                 ELSE ''
                                 END
                             ) AS acta_falsa_perulibre,
                         (
                             CASE
                                 WHEN esc3_prob_fp < 0.1 THEN (
                                     CASE
                                         WHEN NOT cumplimiento_esc2_fp
                                             THEN 'Hallazgo'
                                         WHEN NOT cumplimiento_esc1_fp
                                             THEN 'Sospechoso'
                                         ELSE ''
                                         END
                                     )
                                 ELSE ''
                                 END
                             ) AS acta_falsa_fp
                  FROM (
                           SELECT *,
                                  v2_perulibre * 1.1 > mm_perulibre * 0.9                AS cumplimiento_mm_perulibre,
                                  v2_fp * 1.1 > mm_fp * 0.9                              AS cumplimiento_mm_fp,
                                  FLOOR(mm_perulibre * 0.5)                              AS esc1_perulibre,
                                  FLOOR(mm_fp * 0.5)                                     AS esc1_fp,
                                  v2_perulibre > FLOOR(mm_perulibre * 0.5)               AS cumplimiento_esc1_perulibre,
                                  v2_fp > FLOOR(mm_fp * 0.5)                             AS cumplimiento_esc1_fp,
                                  FLOOR(mm_perulibre * 0.2)                              AS esc2_perulibre,
                                  FLOOR(mm_fp * 0.2)                                     AS esc2_fp,
                                  v2_perulibre > FLOOR(mm_perulibre * 0.2)               AS cumplimiento_esc2_perulibre,
                                  v2_fp > FLOOR(mm_fp * 0.2)                             AS cumplimiento_esc2_fp,
                                  CASE
                                      WHEN v2_perulibre >= mm_perulibre THEN 0
                                      ELSE mm_perulibre - v2_perulibre END               AS delta_perulibre,
                                  CASE WHEN v2_fp >= mm_fp THEN 0 ELSE mm_fp - v2_fp END AS delta_fp,
                                  CASE
                                      WHEN v2_perulibre >= mm_perulibre THEN 1
                                      ELSE POWER(0.8, mm_perulibre - v2_perulibre) END   AS esc3_prob_perulibre,
                                  CASE
                                      WHEN v2_fp >= mm_fp THEN 1
                                      ELSE POWER(0.8, mm_fp - v2_fp) END                 AS esc3_prob_fp
                           FROM (
                                    SELECT p.mesa,
                                           p.v2_CCODI_UBIGEO,
                                           p.v2_DEPARTAMENTO,
                                           p.v2_PROVINCIA,
                                           p.v2_DISTRITO,
                                           p.v2_perulibre,
                                           p.v2_fp,
                                           p.v2_blanco,
                                           p.v2_nulos,
                                           p.v2_OBSERVACION,
                                           p.v2_OBSERVACION_TXT,
                                           FLOOR(
                                                           p.v1_pnp * r.r_pnp_perulibre +
                                                           p.v1_fa * r.r_fa_perulibre +
                                                           p.v1_morado * r.r_morado_perulibre +
                                                           p.v1_pps * r.r_pps_perulibre +
                                                           p.v1_vn * r.r_vn_perulibre +
                                                           p.v1_ap * r.r_ap_perulibre +
                                                           p.v1_avpais * r.r_avpais_perulibre +
                                                           p.v1_pp * r.r_pp_perulibre +
                                                           p.v1_jpp * r_jpp_perulibre +
                                                           p.v1_ppc * r_ppc_perulibre +
                                                           p.v1_fp * r_fp_perulibre +
                                                           p.v1_upp * r_upp_perulibre +
                                                           p.v1_rp * r_rp_perulibre +
                                                           p.v1_runa * r_runa_perulibre +
                                                           p.v1_sp * r_sp_perulibre +
                                                           p.v1_perulibre * r_perulibre_perulibre +
                                                           p.v1_dd * r_dd_perulibre +
                                                           p.v1_app * r_app_perulibre +
                                                           0
                                               ) AS mm_perulibre,
                                           FLOOR(
                                                           p.v1_pnp * r.r_pnp_fp +
                                                           p.v1_fa * r.r_fa_fp +
                                                           p.v1_morado * r.r_morado_fp +
                                                           p.v1_pps * r.r_pps_fp +
                                                           p.v1_vn * r.r_vn_fp +
                                                           p.v1_ap * r.r_ap_fp +
                                                           p.v1_avpais * r.r_avpais_fp +
                                                           p.v1_pp * r.r_pp_fp +
                                                           p.v1_jpp * r_jpp_fp +
                                                           p.v1_ppc * r_ppc_fp +
                                                           p.v1_fp * r_fp_fp +
                                                           p.v1_upp * r_upp_fp +
                                                           p.v1_rp * r_rp_fp +
                                                           p.v1_runa * r_runa_fp +
                                                           p.v1_sp * r_sp_fp +
                                                           p.v1_perulibre * r_perulibre_fp +
                                                           p.v1_dd * r_dd_fp +
                                                           p.v1_app * r_app_fp +
                                                           0
                                               ) AS mm_fp
                                    FROM presidencial AS p
                                             INNER JOIN ratios_de_payaso2 AS r ON p.v2_DEPARTAMENTO = r.departamento
                                )
                       )
              )
     );