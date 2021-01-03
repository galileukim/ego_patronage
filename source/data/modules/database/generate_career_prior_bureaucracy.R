# ==============================================================================
# input: sql database of party affiliation and rais_entries
# output: generate summary tables of observables for partisans and non-partisans
# before they enter the bureaucracy
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
)

filiado <- tbl(rais_con, "filiado_mun")

career_prior_to_bureaucracy <- dbGetQuery(
    "
    SELECT
        rais.cod_ibge_6,
        rais.year,
        rais.id_employee,
        cbo_02,
        age,
        edu,
        wage,
        work_experience,
        filiado_mun.party,
        filiado_mun.date_start,
        filiado_mun.date_end,
        filiado_mun.date_cancel
    FROM rais
    LEFT JOIN 
        (
             SELECT * FROM filiado_mun
             GROUP BY filiado_mun.id_employee
             HAVING filiado_mun.date_start = MIN(filiado_mun.date_start)
        ) AS filiado_mun
        ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6
        AND rais.id_employee = filiado_mun.id_employee
    INNER JOIN rais_bureaucrat_entry
        ON (rais.cod_ibge_6 = rais_bureaucrat_entry.cod_ibge_6
        AND rais.id_employee = rais_bureaucrat_entry.id_employee
        AND rais.year <= rais_bureaucrat_entry.year)
    GROUP BY rais.id_employee
    HAVING rais.year = MAX(rais.year)
    "
)

message("join data with partisan affiliation")
career_filiado <- career_prior_to_bureaucracy %>%
    mutate(cod_ibge_6 = as.character(cod_ibge_6)) %>%
    generate_year_filiado() %>%
    classify_partisanship()

# ---------------------------------------------------------------------------- #
# compute summary statistics for partisans and non-partisans
message("computing summary statistics...")

group_vars <- c("cod_ibge_6", "year", "is_partisan")

career_filiado_mean <- career_filiado %>%
    compute_summary_stats(
        all_of(group_vars),
        c(age, work_experience, edu)
    )

career_filiado_median <- career_filiado %>%
    group_by(
        across(all_of(group_vars))
    ) %>%
    compute_median(wage)

career_filiado_summary <- career_filiado_mean %>%
    left_join(
        career_filiado_median,
        by = group_vars
    ) %>%
    filter(!is.na(is_partisan))

# ---------------------------------------------------------------------------- #
message("uploading to sql database...")

table_name <- "career_pre_bureaucracy_mun"

if(dbExistsTable(rais_con, table_name)){
    dbRemoveTable(rais_con, table_name)
}

dbWriteTable(
    rais_con,
    table_name,
    career_filiado_summary
)

message("module complete: generate summary tables career pre-bureaucracy!")
