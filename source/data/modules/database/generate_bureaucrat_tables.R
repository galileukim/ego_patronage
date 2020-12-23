# ==============================================================================
# input: rais sql database, micro-level
# output: set of tables 1) all former, current and future bureaucrats
# 2) date of each entry into the bureaucracy
# 3) date of each exit into the bureaucracy
# requirement: reduce query times by generating readily available tables
# ==============================================================================
debug <- FALSE

source(
    here::here("source/data/modules/database/requirements.R")
)

RSQLite::initExtension(rais_con)

sprintf(
    "DROP TABLE IF EXISTS %s", 
    c("rais_bureaucrat_id", "rais_bureaucrat_entry", "rais_bureaucrat_exit")
) %>%
    walk(dbExecute)

message("generate new table records")

dbExecute(
    "
    CREATE TABLE IF NOT EXISTS rais_bureaucrat_id AS
    SELECT DISTINCT id_employee FROM
    rais WHERE nat_jur = 1031
    "
)

dbExecute(
    "
    CREATE TABLE IF NOT EXISTS rais_bureaucrat_entry AS
    SELECT cod_ibge_6 id_employee, year
    FROM rais 
    WHERE type_admission > 0 and nat_jur = 1031
    GROUP BY id_employee
    HAVING year = MIN(year)
    "
 )

dbExecute(
    "
    CREATE TABLE IF NOT EXISTS rais_bureaucrat_exit AS
    SELECT cod_ibge_6 id_employee, year
    FROM rais 
    WHERE cause_fired > 0 and nat_jur = 1031
    GROUP BY id_employee
    HAVING year = MIN(year)
    "
 )

message("write out tables to sqlite complete!")
