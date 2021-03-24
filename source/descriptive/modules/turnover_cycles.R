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

plot_partisan_stat <- function(data, var){
     ggplot(
        aes_string("year", var, color = "is_filiado"),
        data = data
    ) +
    stat_summary_bin(
        fun = "mean",
        geom = "point"
    ) +
    stat_summary_bin(
        fun = "mean",
        geom = "line"
    ) +
    ggtitle(var)
}

rais <- tbl(rais_con, "rais")

rais_tables <- list.files(
    here("data/clean/summary/"),
    pattern = "^rais",
    full.names = TRUE
)

rais_filiado <- map_dfr(
    rais_tables,
    fread,
    .id = "is_filiado"
) %>%
    mutate(
        is_filiado = recode(is_filiado, `1` = "non-partisan", `2` = "partisan")
    )

message("generate plots")
vars_to_plot <- c("hired", "fired", "age", "edu", "wage", "work_experience")
plot_turnover <- map(
    vars_to_plot,
    ~ plot_partisan_stat(var = ., data = rais_filiado)
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
