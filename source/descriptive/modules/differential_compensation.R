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
        cbo_02,
        age,
        edu,
        gender,
        nat_jur,
        wage,
        work_experience,
        is_filiado,
        year_start,
        year_end
    FROM rais
    INNER JOIN rais_bureaucrat_entry
        ON (rais.cod_ibge_6 = rais_bureaucrat_entry.cod_ibge_6
        AND rais.id_employee = rais_bureaucrat_entry.id_employee
        AND rais.year <= rais_bureaucrat_entry.year)
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
)

# extract last_job and combine it with data from filiado
