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
    partisan = "rais_mun_partisan.csv.gz",
    non_partisan = "rais_mun_non_partisan.csv.gz"
)

rais_filiado <- rais_mun_tables %>%
    map_dfr(
        ~read_data(., type = "clean", dir = "summary"),
        .id = "is_partisan"
    )

outcome_vars <- c(
    Age = "age", 
    Education = "edu",
    Dismissals = "fired",
    Hires = "hired",
    Experience = "work_experience",
    Wage = "wage"
)

# # compute statistics per year
# rais_filiado_summary <- rais_filiado %>%
#     group_by(is_partisan, year) %>%
#     summarise(
#         across(
#             c("age", "edu", "fired", "hired", "work_experience"),
#             mean, na.rm = TRUE
#             ),
#             wage = median(wage, na.rm = TRUE)
#     )

plot_descriptive <- map2(
        outcome_vars,
        names(outcome_vars),
        ~ ggplot(
            aes_string("year", .x, color = "is_partisan"),
            data = rais_filiado
        ) +
        geom_smooth() +
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

