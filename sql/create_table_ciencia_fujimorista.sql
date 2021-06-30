CREATE TABLE ciencia_fujimorista AS
SELECT r1.departamento,
       ABS(ROUND(r1.r_pnp_fp - r2.r_pnp_fp, 1)) * 100 AS pnp,
       ABS(ROUND(r1.r_fa_fp - r2.r_fa_fp, 1)) * 100 AS fa,
       ABS(ROUND(r1.r_morado_fp - r2.r_morado_fp, 1)) * 100 AS morado,
       ABS(ROUND(r1.r_pps_fp - r2.r_pps_fp, 1)) * 100 AS pps,
       ABS(ROUND(r1.r_vn_fp - r2.r_vn_fp, 1)) * 100 AS vn,
       ABS(ROUND(r1.r_ap_fp - r2.r_ap_fp, 1)) * 100 AS ap,
       ABS(ROUND(r1.r_avpais_fp - r2.r_avpais_fp, 1)) * 100 AS avpais,
       ABS(ROUND(r1.r_pp_fp - r2.r_pp_fp, 1)) * 100 AS pp,
       ABS(ROUND(r1.r_jpp_fp - r2.r_jpp_fp, 1)) * 100 AS jpp,
       ABS(ROUND(r1.r_ppc_fp - r2.r_ppc_fp, 1)) * 100 AS ppc,
       ABS(ROUND(r1.r_fp_fp - r2.r_fp_fp, 1)) * 100 AS fp,
       ABS(ROUND(r1.r_upp_fp - r2.r_upp_fp, 1)) * 100 AS upp,
       ABS(ROUND(r1.r_rp_fp - r2.r_rp_fp, 1)) * 100 AS rp,
       ABS(ROUND(r1.r_runa_fp - r2.r_runa_fp, 1)) * 100 AS runa,
       ABS(ROUND(r1.r_sp_fp - r2.r_sp_fp, 1)) * 100 AS sp,
       ABS(ROUND(r1.r_perulibre_fp - r2.r_perulibre_fp, 1)) * 100 AS perulibre,
       ABS(ROUND(r1.r_dd_fp - r2.r_dd_fp, 1)) * 100 AS dd,
       ABS(ROUND(r1.r_app_fp - r2.r_app_fp, 1)) * 100 AS app
FROM ratios_de_payaso AS r1 INNER JOIN ratios_de_payaso2 AS r2 on r1.departamento = r2.departamento
ORDER BY r1.departamento;