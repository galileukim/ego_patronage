# ==============================================================================
# input: rais municipal sql tables
# output: descriptive statistics on partisan vs. non-partisan
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
)

# ==============================================================================
# load-in data
# ==============================================================================
rais_mun_tables <- c(
    partisan = "rais_mun_partisan",
    non_partisan = "rais_mun_non_partisan"
)

rais_filiado <- rais_mun_tables %>%
    map_dfr(
        ~ tbl(rais_con, .) %>%
            mutate(year = as.integer(year)) %>%
            collect(),
        .id = "is_partisan"
    )

outcome_vars <- c(
    Age = "age", 
    Education = "edu",
    Dismissals = "fired",
    Hires = "hired",
    `Work Experience` = "work_experience",
    Wage = "wage"
)

plot_descriptive <- map2(
        outcome_vars,
        names(outcome_vars),
        ~ gg_point_line(
            aes_string("year", .x, color = "is_partisan"),
            data = rais_filiado
        ) +
        guides(color = guide_legend("Partisanship")) +
        labs(x = "Year", y = .y)
    )

message("exporting plots")
plot_filenames <- generate_plot_filenames(
    "paper/figures/active_employee/",
    outcome_vars,
    debug
)

pmap(
    list(
        plot = plot_descriptive,
        filename = plot_filenames
    ),
    ggsave
)

message("module completed: generate descriptive statistics for active employees.")

