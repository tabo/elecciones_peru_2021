create table payasada
(
    mesa TEXT,
    v2_CCODI_UBIGEO TEXT,
    v2_DEPARTAMENTO TEXT,
    v2_PROVINCIA TEXT,
    v2_DISTRITO TEXT,
    v2_perulibre NUM,
    v2_fp NUM,
    v2_blanco NUM,
    v2_nulos NUM,
    v2_OBSERVACION TEXT,
    v2_OBSERVACION_TXT TEXT,
    mm_perulibre,
    mm_fp_payaso,
    mm_fp_corregido,
    cumplimiento_mm_perulibre,
    cumplimiento_mm_fp_payaso,
    cumplimiento_mm_fp_corregido,
    esc1_perulibre,
    esc1_fp_payaso,
    esc1_fp_corregido,
    cumplimiento_esc1_perulibre,
    cumplimiento_esc1_fp_payaso,
    cumplimiento_esc1_fp_corregido,
    esc2_perulibre,
    esc2_fp_payaso,
    esc2_fp_corregido,
    cumplimiento_esc2_perulibre,
    cumplimiento_esc2_fp_payaso,
    cumplimiento_esc2_fp_corregido,
    delta_perulibre,
    delta_fp_payaso,
    delta_fp_corregido,
    esc3_prob_perulibre,
    esc3_prob_fp_payaso,
    esc3_prob_fp_corregido,
    esc3_desc_perulibre,
    esc3_desc_fp_payaso,
    esc3_desc_fp_corregido,
    alerta_por_nulos_y_blancos,
    acta_falsa_perulibre,
    acta_falsa_fp_payaso,
    acta_falsa_fp_corregido,
    anulada_payaso,
    anulada_corregido,
    hallazgo_perulibre_payaso,
    hallazgo_perulibre_corregido,
    sospechoso_perulibre_payaso,
    sospechoso_perulibre_corregido,
    hallazgo_fp_payaso,
    hallazgo_fp_corregido,
    sospechoso_fp_payaso,
    sospechoso_fp_corregido,
    PRIMARY KEY (mesa),
    FOREIGN KEY(mesa) REFERENCES mesas_20210411(mesa),
    FOREIGN KEY(mesa) REFERENCES mesas_20210606(mesa)
);