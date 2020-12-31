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

rais_filiado <- dbGetQuery(
    "
    SELECT 
        rais.cod_ibge_6,
        rais.year,
        rais.id_employee,
        rais.cbo_02,
        rais.cnae_95,
        rais.edu,
        rais.hours,
        rais.wage,
        rais.work_experience,
        rais.contract_type,
        filiado_mun.filiado_id,
        filiado_mun.date_start,
        filiado_mun.date_end,
        filiado_mun.is_filiado
    FROM rais
    LEFT JOIN (SELECT *, 1.0 AS is_filiado FROM filiado_mun) AS filiado_mun
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    "
)

rais_filiado <- tbl(rais_con, "rais") %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    ) %>%
    mutate(
        cbo_group_detail = case_when(
            occupation == 0 ~ "army",
            `1` = "executive",
            `2` = "liberal arts",
            `3` = "engineer",
            `4` = "administration",
            `5` = "services",
            `6` = "agriculture",
            `7` = "industry",
            `8` = "industry",
            `9` = "maintenance"
        )
    )

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
        dir = "summary_tables",
        file = "partisan_dominance.rds"
    )

# message("generate party dummy and compute partisanship by group")
# rais_filiado_occupation <- rais_filiado %>%
#     mutate(
#         occupation = str_sub(cbo_02, 1, 1)
#     ) %>%
#     fix_occupation() %>%
#     fix_edu() %>%
#     filter(
#         !is.na(cbo_group) & !is.na(cbo_group_detail)
#     )

# # ==============================================================================
# # compute party dominance across different categories
# # ==============================================================================
# message("computing dominance...")


# partisan_summary <- map(
#     group_vars,
#     ~ compute_summary_stats(
#         data = rais_filiado_occupation,
#         .group_vars = all_of(.),
#         .summary_vars = c(is_partisan)
#     )
# ) %>%
#     set_names(group_vars)