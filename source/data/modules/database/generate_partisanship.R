# ==============================================================================
# input: sql database of party affiliation
# output: assess extent to which there is party dominance
# ==============================================================================
source(
    here::here("source/data/modules/database/globals.R")
)

source(
    here("source/data/modules/database/requirements.R")
)

message("extract records and join with filiado data")

group_vars <- c(
    "SUBSTR(cbo_02, 1, 1)", "contract_type", "edu"
)

rais_query <- sprintf(
    "
    SELECT
    rais.cod_ibge_6,
    rais.year,
    %1$s,
    SUM(1.0) AS n,
    AVG(COALESCE(is_filiado, 0)) AS partisanship
    FROM rais
    LEFT JOIN (SELECT *, 1.0 AS is_filiado FROM filiado_mun) AS filiado_mun
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    GROUP BY rais.cod_ibge_6, rais.year, %1$s
    ",
    group_vars
)

rais_filiado <- map(
    rais_query,
    dbGetQuery
) %>%
    set_names(
        c("occupation", "contract", "education")
    )

# fix grouping columns
rais_filiado <- rais_filiado %>%
    modify_at(
        "occupation",
        ~ rename(., occupation = `SUBSTR(cbo_02, 1, 1)`) %>%
            fix_occupation() %>%
            filter(!is.na(cbo_group))
    ) %>%
    modify_at(
        "education",
        fix_edu
    )

rais_filiado %>%
    write_data(
        dir = "data/clean/summary",
        file = "partisan_dominance.rds"
    )
