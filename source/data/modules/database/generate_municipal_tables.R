# ==============================================================================
# input: rais sql database, micro-level
# output: municipal level table of summary statistics
# requirement: reduce query times by generating readily available tables
# ==============================================================================
debug <- TRUE
RSQLite::initExtension(rais_con)
vars_to_plot <- c("hired", "fired", "age", "edu", "work_experience")

source(
    here::here("source/data/modules/database/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")

rais_partisan <- rais %>%
    anti_join(filiado) %>%
    group_by(
        cod_ibge_6, year
    ) %>%
    summarise(
        across(
            vars_to_plot,
            list(mean = mean, median = median),
            na.rm = TRUE
        )
    )

rais_partisan <- dbGetQuery(
    rais_con,
    "
    SELECT
    rais.cod_ibge_6,
    rais.year,
    AVG(hired) AS hired,
    AVG(fired) AS fired,
    AVG(age) AS age,
    AVG(EDU) AS edu,
    AVG(work_experience) AS work_experience,
    MEDIAN(wage) AS wage
    FROM rais
    LEFT JOIN filiado_mun
    ON rais.id_employee = filiado_mun.id_employee
    WHERE filiado_mun.id_employee IS NULL
    GROUP BY rais.cod_ibge_6
    "
)
