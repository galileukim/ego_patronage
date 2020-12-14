# ==============================================================================
# input: rais sql database, micro-level
# output: set of tables 1) all former, current and future bureaucrats
# 2) date of each entry into the bureaucracy
# 3) date of each exit into the bureaucracy
# requirement: reduce query times by generating readily available tables
# ==============================================================================
debug <- TRUE

source(
    here::here("source/data/modules/database/requirements.R")
)

RSQLite::initExtension(rais_con)

message("begin extracting records")

# rais_bureaucrat_id <- dbGetQuery(
#     rais_con,
#     "
#     SELECT DISTINCT id_employee FROM
#     rais WHERE nat_jur = 1031
#     "
# )

rais_bureaucrat_entries <- dbGetQuery(
    rais_con,
    "
    SELECT cod_ibge_6, year, id_employee
    FROM (
        SELECT *,
        FIRST_VALUE(year) OVER 
        (PARTITION BY id_employee ORDER BY year) AS min_year
        FROM rais
    )
    WHERE hired = 1 AND nat_jur = 1031 AND year = min_year
    ORDER BY id_employee, year
    "
)

rais_bureaucrat_exits <- dbGetQuery(
    rais_con,
    "
    SELECT cod_ibge_6, year, id_employee
    FROM (
        SELECT *, 
        FIRST_VALUE(year) OVER 
        (PARTITION BY id_employee ORDER BY year DESC) AS min_year
        FROM rais
    )
    WHERE fired = 1 AND nat_jur = 1031 AND year = min_year
    ORDER BY id_employee, year
    "
)

message("write out tables to sqlite")

sql_tables <- list(
    rais_bureaucrat_entries, rais_bureaucrat_exits
)

sql_table_names <- c(
    "rais_bureaucrat_entries", "rais_bureaucrat_exits"
)

pwalk(
    list(
        name = sql_table_names,
        value = sql_tables
    ),
    dbWriteTable,
    conn = rais_con,
    overwrite = TRUE
)

message("write out tables to sqlite complete!")