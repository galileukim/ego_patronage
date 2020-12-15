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
    SELECT * FROM rais 
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
