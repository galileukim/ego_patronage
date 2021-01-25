# ==============================================================================
# input: rais sql database, micro-level
# output: municipal level table of summary statistics
# requirement: reduce query times by generating readily available tables
# ==============================================================================
source(
    here::here("source/data/modules/database/globals.R")
)

source(
    here("source/data/modules/database/requirements.R")
)

RSQLite::initExtension(rais_con)

# sprintf(
#     "DROP TABLE IF EXISTS %s", c("rais_mun_partisan", "rais_mun_non_partisan")
# ) %>%
#     walk(dbExecute)

message("non partisan")
rais_mun_non_partisan <- dbGetQuery(
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
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    WHERE rais.nat_jur = 1031 AND
    filiado_mun.id_employee IS NULL
    GROUP BY rais.cod_ibge_6, rais.year, SUBSTR(rais.cbo_02, 1, 1)
    "
)

message("partisan")
rais_mun_partisan <- dbGetQuery(
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
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    WHERE rais.nat_jur = 1031 AND
    CAST(rais.year AS INT) >= filiado_mun.year_start AND
    CAST(rais.year AS INT) <= filiado_mun.year_termination
    GROUP BY rais.cod_ibge_6, rais.year, SUBSTR(rais.cbo_02, 1, 1)
    "
)

message("write out tables")
export <- list(
    object = list(rais_mun_partisan, rais_mun_non_partisan) %>%
        map(
            ~ rename(., occupation = `SUBSTR(cbo_02, 1, 1)`) %>%
                fix_occupation()
        ),
    file = c("rais_mun_partisan.csv.gz", "rais_mun_non_partisan.csv.gz")
)

export %>%
    pwalk(write_data, dir = "summary")

message("create municipal tables complete!")