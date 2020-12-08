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

filiado_join <- filiado %>% 
    select(-cod_ibge_6) %>%
    mutate(
        is_filiado = "partisan"
    )

rais_filiado <- rais %>%
    filter(is_municipal) %>%
    select(
        year,
        id_employee,
        hired,
        fired
    ) %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    )  %>%
    left_join(
        filiado_join,
        by = "id_employee"
    ) %>%
    collect()

rais_filiado <- rais_filiado %>%
    mutate(
        is_filiado = coalesce(is_filiado, "non_partisan")
    )

vars_to_plot <- c("hired", "fired")
plot_turnover <- map(
    c("hired", "fired"),
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
path_out <- here(path_to_figs, file_names)

pwalk(
    list(
        filename = path_out,
    plot = plot_turnover
    ),
    ggsave
)
