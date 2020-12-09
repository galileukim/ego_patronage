# ==============================================================================
# input: rais sql database, micro-level
# output: municipal level table of summary statistics
# requirement: reduce query times by generating readily available tables
# ==============================================================================
debug <- FALSE
RSQLite::initExtension(rais_con)
vars_to_plot <- c("hired", "fired", "age", "edu", "work_experience")

source(
    here::here("source/data/modules/database/requirements.R")
)

sprintf(
    "DROP TABLE IF EXISTS %s", c("rais_mun_partisan", "rais_mun_non_partisan")
) %>%
    walk(
        dbExecute,
        conn = rais_con
    )

dbExecute(
    rais_con,
    "
    CREATE TABLE IF NOT EXISTS
    rais_mun_non_partisan AS
    SELECT
    rais.cod_ibge_6,
    rais.year,
    SUBSTR(rais.cbo_02, 1, 1) AS occupation,
    AVG(hired) AS hired,
    AVG(fired) AS fired,
    AVG(age) AS age,
    AVG(EDU) AS edu,
    AVG(work_experience) AS work_experience,
    MEDIAN(wage) AS wage,
    COUNT(*) AS n
    FROM rais
    LEFT JOIN filiado_mun
    ON rais.id_employee = filiado_mun.id_employee
    WHERE filiado_mun.id_employee IS NULL
    GROUP BY rais.cod_ibge_6, rais.year, occupation
    "
)

dbExecute(
    rais_con,
    "
    CREATE TABLE IF NOT EXISTS
    rais_mun_partisan AS
    SELECT
    rais.cod_ibge_6,
    rais.year,
    SUBSTR(rais.cbo_02, 1, 1) AS occupation,
    AVG(hired) AS hired,
    AVG(fired) AS fired,
    AVG(age) AS age,
    AVG(EDU) AS edu,
    AVG(work_experience) AS work_experience,
    MEDIAN(wage) AS wage,
    COUNT(*) AS n
    FROM rais
    INNER JOIN filiado_mun
    ON rais.id_employee = filiado_mun.id_employee
    GROUP BY rais.cod_ibge_6, rais.year, occupation
    "
)
