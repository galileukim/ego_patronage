# ==============================================================================
# input: sql database
# output: descriptive statistics of turnover cycles in bureaucracy
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")

filiado_join <- filiado %>% 
    select(-cod_ibge_6) %>%
    mutate(
        filiado = 1
    )

rais_filiado <- rais %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    ) %>%
    filter(is_municipal) %>%
    left_join(
        filiado_join,
        by = "id_employee"
    )

rais_filiado %>%
    group_by(filiado) %>%
    