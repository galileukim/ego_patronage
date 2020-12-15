# ==============================================================================
# input: sql database of party affiliation
# output: compare differential comps for partisans and non-partisans
# before they enter the bureaucracy
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")
bureaucracy_entry <- tbl(rais_con, "rais_bureaucrat_entry")

career_prior_to_bureaucracy <- dbGetQuery(
    "
    SELECT * FROM rais
        INNER JOIN rais_bureaucrat_entry
        ON (rais.cod_ibge_6 = rais_bureaucrat_entry.cod_ibge_6
        AND rais.id_employee = rais_bureaucrat_entry.id_employee
        AND rais.year <= rais_bureaucrat_entry.year)
        LEFT JOIN filiado_mun
    "
)

# extract last_job and combine it with data from filiado
