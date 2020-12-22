# fix year
debug <- FALSE
source(
    here::here("source/data/modules/database/requirements.R")
)

if (dbExistsTable(rais_con, "rais_correct")) {
    dbRemoveTable(rais_con, "rais_correct")
}

dbExecute(
    "
    CREATE TABLE rais_correct AS
    SELECT 
    id_employee,
    CAST(year AS INT) AS year,
    cod_ibge_6,
    cbo_02,
    cnae_95,
    cnae_20,
    age,
    edu,
    gender,
    race,
    work_experience,
    wage, date_admission,
    date_separation,
    nat_jur,
    hours,
    type_admission,
    cause_fired, hired,fired, departure, contract_type
    FROM rais"
)

if (collect(count(tbl(rais_con, "rais"))) >= 1e6) {
    dbRemoveTable(rais_con, "rais")

    dbExecute(
        "ALTER TABLE rais_correct RENAME TO rais"
    )
}