# ==============================================================================
# input: sql database of party affiliation
# output: compare differential observables for partisans and non-partisans
# before they enter the bureaucracy
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")
bureaucracy_entry <- tbl(rais_con, "rais_bureaucrat_entry")

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
        work_experience
    FROM rais
    INNER JOIN rais_bureaucrat_entry
        ON (rais.cod_ibge_6 = rais_bureaucrat_entry.cod_ibge_6
        AND rais.id_employee = rais_bureaucrat_entry.id_employee
        AND rais.year <= rais_bureaucrat_entry.year)
    "
)

message("join data with partisan affiliation")
career_filiado <- career_prior_to_bureaucracy %>%
    mutate(cod_ibge_6 = as.character(cod_ibge_6)) %>%
    left_join(
        collect(filiado),
        by = c("cod_ibge_6", "id_employee")
    ) %>%
    generate_year_filiado() %>%
    classify_partisanship()

# note that the majority of partisans become party members after
# joining the bureau
career_filiado %>%
    filter(!is.na(is_partisan)) %>%
    ggplot() +
    geom_bar(
        aes(is_partisan),
        stat = "count"
    )

# ---------------------------------------------------------------------------- #
# compute summary statistics for partisans and non-partisans
message("computing summary statistics...")
group_vars <- c("year", "is_partisan")

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

message("complete!")

# ---------------------------------------------------------------------------- #
message("generating descriptive plots")

vars_to_plot <- c("age", "work_experience", "edu") %>%
    sprintf(fmt = "mean_%s", .) %>%
    c("median_wage")

plot_summary <- map(
    vars_to_plot,
    ~ gg_point_line(
        data = career_filiado_summary,
        aes_string("year", ., color = "is_partisan")
    )
)

plot_filenames <- generate_plot_filenames(
    "paper/figures/partisanship/",
    vars_to_plot,
    debug
)

pmap(
    list(
        plot = plot_summary,
        filename = plot_filenames
    ),
    ggsave
)

message("completed: generating plots for prior to bureaucracy.")