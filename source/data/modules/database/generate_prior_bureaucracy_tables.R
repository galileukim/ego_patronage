# ==============================================================================
# input: filiado data and rais, rais_bureaucracy tables
# output: summary statistics of observable covariates for partisans
# ==============================================================================
source(
    here::here("source/data/modules/database/globals.R")
)

source(
    here("source/data/modules/database/requirements.R")
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
        gender,
        nat_jur,
        wage,
        work_experience,
        filiado_mun.party,
        filiado_mun.date_start,
        filiado_mun.date_end,
        filiado_mun.date_cancel
    FROM rais
    LEFT JOIN filiado_mun
        ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6
        AND rais.id_employee = filiado_mun.id_employee
    INNER JOIN rais_bureaucrat_entry
        ON (rais.cod_ibge_6 = rais_bureaucrat_entry.cod_ibge_6
        AND rais.id_employee = rais_bureaucrat_entry.id_employee
        AND rais.year <= rais_bureaucrat_entry.year)
    "
)

message("join data with partisan affiliation")
career_filiado <- career_prior_to_bureaucracy %>%
    mutate(cod_ibge_6 = as.character(cod_ibge_6)) %>%
    generate_year_filiado() %>%
    classify_partisanship()

# note that the majority of partisans become party members after
# joining the bureau
plot_breakdown <- career_filiado %>%
    filter(!is.na(is_partisan)) %>%
    ggplot() +
    geom_bar(
        aes(is_partisan),
        stat = "count"
    )

# ---------------------------------------------------------------------------- #
# compute summary statistics for partisans and non-partisans
message("computing summary statistics...")
group_vars <- c("cod_ibge_6", "year", "is_partisan")

career_filiado_mean <- career_filiado %>%
    compute_mean(
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
message("uploading summary statistics to sql data.")


message("module complete: generate summary statistics prior to entry!")
