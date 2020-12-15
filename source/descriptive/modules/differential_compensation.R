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
    SELECT
        rais.cod_ibge_6,
        rais.year,
        rais.id_employee,
        cbo_02,
        age,
        edu,
        gender,
        nat_jur,
        wage,
        work_experience
    FROM rais
    INNER JOIN rais_bureaucrat_entry
        ON (rais.cod_ibge_6 = rais_bureaucrat_entry.cod_ibge_6
        AND rais.id_employee = rais_bureaucrat_entry.id_employee
        AND rais.year <= rais_bureaucrat_entry.year)
    "
)

career_prior_filiado <- career_prior_to_bureaucracy %>%
    mutate(cod_ibge_6 = as.character(cod_ibge_6)) %>%
    left_join(
        collect(filiado),
        by = c("cod_ibge_6", "id_employee")
    ) %>%
    mutate(
        across(starts_with("date"),
            ~ str_sub(., 1, 4) %>% as.integer(),
            .names = "year_{col}"
        )
    ) %>%
    mutate(
        prior_partisan = if_else(
            year >= year_date_start &
                year <= year_date_end, 1L, 0L
        )
    )

"
    LEFT JOIN (
            SELECT 
                cod_ibge_6,
                id_employee,
                1.0 AS is_filiado,
                SUBSTR(date_start, 1, 4) AS year_start,
                SUBSTR(date_end, 1, 4) AS year_end
                FROM filiado_mun
            ) AS filiado
        ON (rais.id_employee = filiado.id_employee AND
        rais.cod_ibge_6 = filiado.cod_ibge_6 AND 
        rais.year >= year_start AND rais.year <= year_end)
        "
# figure out why there are so few entries
# extract last_job and combine it with data from filiado