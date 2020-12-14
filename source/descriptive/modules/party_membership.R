# ==============================================================================
# input: sql database of party affiliation
# output: descriptive statistics of turnover cycles in bureaucracy
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")

rais_tables <- c(
    partisan = "rais_mun_partisan", 
    non_partisan = "rais_mun_non_partisan"
)

rais_filiado <- map_dfr(
    rais_tables,
    ~ dbGetQuery(rais_con, sprintf("SELECT * FROM %s", .)),
    .id = "is_filiado"
)