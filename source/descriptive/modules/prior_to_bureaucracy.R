# ==============================================================================
# input: sql database of party affiliation
# output: compare differential observables for partisans and non-partisans
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
        gender,
        nat_jur,
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

# note that the majority of partisans become party members after
# joining the bureau
plot_breakdown <- career_filiado %>%
    filter(!is.na(is_partisan)) %>%
    ggplot() +
    geom_bar(
        aes(is_partisan),
        stat = "count"
    )

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

message("uploading summary statistics to sql data.")

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

plot_summary[["breakdown"]] <- plot_breakdown

plot_filenames <- generate_plot_filenames(
    "paper/figures/partisanship/",
    c(vars_to_plot, "breakdown"),
    debug
)

pmap(
    list(
        plot = plot_summary,
        filename = plot_filenames
    ),
    ggsave
)

message("module completed: generating plots for prior to bureaucracy.")