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
career_filiado <- tbl(rais_con, "career_pre_bureaucracy_mun") %>%
    collect()

# # ---------------------------------------------------------------------------- #
# # compute summary statistics for partisans and non-partisans
# message("computing summary statistics...")

# # note that the majority of partisans become party members after
# # joining the bureau
# plot_breakdown <- career_filiado %>%
#     group_by(year, is_partisan) %>%
#     summarise(n = sum(n)) %>%
#     gg_point_line(
#         aes(year, n/1e6, color = is_partisan)
#     ) +
#     scale_color_discrete(
#         name = "Partisanship",
#         labels = c(
#             "pre_partisan" = "pre-partisan",
#             "post_partisan" = "post-partisan",
#             "non_partisan" = "non-partisan"
#         )
#     ) +
#     labs(x = "Year", y = "Total (millions)")

# group_vars <- c("year", "is_partisan")

# career_filiado_mean <- career_filiado %>%
#     compute_mean(
#         all_of(group_vars),
#         c(age, work_experience, edu)
#     )

# career_filiado_median <- career_filiado %>%
#     group_by(
#         across(all_of(group_vars))
#     ) %>%
#     compute_median(wage)

# career_filiado_summary <- career_filiado_mean %>%
#     left_join(
#         career_filiado_median,
#         by = group_vars
#     ) %>%
#     filter(!is.na(is_partisan))

# message("complete!")

# ---------------------------------------------------------------------------- #
message("generating descriptive plots")

vars_to_plot <- c("age", "work_experience", "edu") %>%
    sprintf(fmt = "%s_mean", .) %>%
    c("median_wage", "n")

label_vars <- c("Age", "Work Experience", "Education", "Wage", "Total (million)")

plot_summary <- map2(
    vars_to_plot,
    label_vars,
    ~ gg_point_line(
        data = career_filiado,
        aes_string("year", .x, color = "is_partisan")
    ) +
    scale_colour_discrete(
        name = "Partisanship",
        labels = c(
            "pre_partisan" = "pre-partisan",
            "post_partisan" = "post-partisan",
            "non_partisan" = "non-partisan"
        )
    ) +
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