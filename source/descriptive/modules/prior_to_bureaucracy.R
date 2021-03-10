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

career_filiado <- tbl(rais_con, "career_pre_bureaucracy_mun") %>%
    collect() %>%
    mutate(year = as.numeric(year))

career_filiado <- career_filiado %>%
    filter(
        is_partisan != "post_partisan"
    ) %>%
    mutate(
        is_partisan = recode(
            is_partisan,
            "non_partisan" = "non_partisan",
            "pre_partisan" = "partisan"
        )
    )

# aggregate
career_filiado_year <- career_filiado %>%
    group_by(year, is_partisan) %>%
    summarise(
        across(
            c(ends_with("mean"), median_wage),
        ~weighted.mean(., n)
        )
    )
# ---------------------------------------------------------------------------- #
message("generating descriptive plots")

vars_to_plot <- c("age", "work_experience", "edu") %>%
    sprintf(fmt = "%s_mean", .) %>%
    c("median_wage", "n")

label_vars <- c(
    "Age", "Work Experience", "Education", "Wage", "Total (million)"
)

plot_summary <- map2(
    vars_to_plot,
    label_vars,
    ~ ggplot(
        data = career_filiado_year,
        aes_string(x = "year", y = .x, color = "is_partisan")
    ) +
    geom_point() +
    geom_line() +
    scale_colour_discrete(
        name = "Partisanship",
        labels = c(
            "pre_partisan" = "pre-partisan",
            "post_partisan" = "post-partisan",
            "non_partisan" = "non-partisan"
        )
    ) +
    geom_vline(xintercept = seq(2005, 2013, 4), lty = "dotted") +
    labs(x = "Year", y = .y)
)

plot_filenames <- generate_plot_filenames(
    "paper/figures/partisanship/",
    c(vars_to_plot),
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
