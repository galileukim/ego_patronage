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
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6, 
    rais.id_employee = filiado_mun.id_employee
    WHERE rais.nat_jur = 1031 AND filiado_mun.id_employee IS NULL
    GROUP BY rais.cod_ibge_6, rais.year
    "
)

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
    WHERE rais.nat_jur = 1031
    GROUP BY rais.cod_ibge_6, rais.year
    "
)

message("write out tables")
rais_mun_non_partisan %>%
    write_data(
        dir = "summary",
        "rais_mun_non_partisan.csv"
    )

rais_mun_partisan <- dbGetQuery(
    "
    SELECT
        rais.cod_ibge_6,
        rais.year,
        rais.hired,
        rais.fired,
        rais.age,
        rais.edu,
        rais.work_experience,
        rais.wage,
        filiado_mun.party,
        filiado_mun.date_start,
        filiado_mun.date_end,
        filiado_mun.date_cancel
    FROM rais
    INNER JOIN filiado_mun
    ON rais.id_employee = filiado_mun.id_employee
    WHERE rais.nat_jur = 1031
    "
)

message("create municipal tables complete!")
