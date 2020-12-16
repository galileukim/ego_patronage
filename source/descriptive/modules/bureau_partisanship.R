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
        rais.cbo_02,
        rais.cnae_95,
        rais.wage,
        rais.contract_type,
        filiado_mun.date_start,
        filiado_mun.date_end
    FROM rais
    LEFT JOIN filiado_mun
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    "
)

rais_filiado <- rais_filiado %>%
    generate_year_filiado() %>%
    filter(
        is.na(filiado_id) |
        year >= year_date_start & year <= year_date_end
    )
