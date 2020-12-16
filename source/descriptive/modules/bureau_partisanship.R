# ==============================================================================
# input: sql database of party affiliation
# output: assess extent to which there is party dominance
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais_filiado <- dbGetQuery(
    "
    SELECT 
        rais.cod_ibge_6,
        rais.year,
        rais.id_employee,
        rais.cbo_02,
        rais.cnae_95,
        rais.wage,
        rais.contract_type,
        filiado_mun.filiado_id,
        filiado_mun.date_start,
        filiado_mun.date_end,
        filiado_mun.is_filiado
    FROM rais
    LEFT JOIN (SELECT *, 1.0 AS is_filiado FROM filiado_mun) AS filiado_mun
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    "
)

rais_filiado %>%
    generate_year_filiado() %>%
    filter(
        is.na(filiado_id) |
        !!is_partisan
    )