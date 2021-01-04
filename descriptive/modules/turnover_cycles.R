# ==============================================================================
# input: sql database
# output: descriptive statistics of turnover cycles in bureaucracy
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")

rais_tables <- c(
    partisan = "rais_mun_partisan",
    non_partisan = "rais_mun_non_partisan"
)

rais_filiado <- map_dfr(
    rais_tables,
    ~ dbGetQuery(sprintf("SELECT * FROM %s", .)),
    .id = "is_filiado"
)

message("generate plots")
vars_to_plot <- c("hired", "fired", "work_experience")
plot_turnover <- map(
    vars_to_plot,
    ~ gg_summary(
        x = "year",
        y = .,
        data = rais_filiado,
        smooth = FALSE,
        color = !!sym("is_filiado")
    ) +
        ggtitle(.)
)

rais_filiado %>%
    ggplot(
        aes(year, hired, color = is_filiado)
    ) +
    stat_summary_bin(
        fun = "mean",
        geom = "point"
    )

path_to_figs <- ifelse(
    isTRUE(debug), "paper/figures/turnover/sample/", "paper/figures/turnover/"
)

file_names <- sprintf(
    "plot_%s.pdf", vars_to_plot
)

message("save plots")
path_out <- here(path_to_figs, file_names)
pwalk(
    list(
        filename = path_out,
        plot = plot_turnover
    ),
    save_plot
)
