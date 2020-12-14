# ==============================================================================
# input: rais sql database, micro-level
# output: municipal level table of summary statistics
# requirement: reduce query times by generating readily available tables
# ==============================================================================
debug <- FALSE

source(
    here::here("source/data/modules/database/requirements.R")
)

RSQLite::initExtension(rais_con)

sprintf(
    "DROP TABLE IF EXISTS %s", c("rais_mun_partisan", "rais_mun_non_partisan")
) %>%
    walk(
        dbExecute,
        conn = rais_con
    )

rais_mun_non_partisan <- dbGetQuery(
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
        MEDIAN(wage) AS wage,
        COUNT(*) AS n
    FROM rais
    LEFT JOIN filiado_mun
    ON rais.id_employee = filiado_mun.id_employee
    WHERE rais.nat_jur = 1031 AND filiado_mun.id_employee IS NULL
    GROUP BY rais.cod_ibge_6, rais.year
    "
)

rais_mun_partisan <- dbGetQuery(
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
        MEDIAN(wage) AS wage,
        COUNT(*) AS n
    FROM rais
    INNER JOIN filiado_mun
    ON rais.id_employee = filiado_mun.id_employee
    WHERE rais.nat_jur = 1031
    GROUP BY rais.cod_ibge_6, rais.year
    "
)

write_tables_to_sql(
    tables = list(rais_mun_non_partisan, rais_mun_partisan),
    names = c("rais_mun_non_partisan", "rais_mun_partisan"),
    conn = rais_con
)
