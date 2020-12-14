# ==============================================================================
# input: sql database
# output: descriptive statistics of turnover cycles in bureaucracy
# ==============================================================================
debug <- TRUE

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
    ~ dbGetQuery(rais_con, sprintf("SELECT * FROM %s", .)),
    .id = "is_filiado"
)

message("generate plots")
vars_to_plot <- c("hired", "fired", "age", "edu", "work_experience", "wage")
plot_turnover <- map(
    vars_to_plot,
    ~ gg_summary(
        x = "year", 
        y = ., 
        data = rais_filiado,
        smooth = FALSE
    ) +
    facet_wrap(. ~ is_filiado)
)

path_to_figs <- "paper/figures/"
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
    ggsave
)
