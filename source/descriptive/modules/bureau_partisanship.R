# ==============================================================================
# input: sql database of party affiliation
# output: assess extent to which there is party dominance
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

message("extract records and join with filiado data")

rais_filiado <- dbGetQuery(
    "
    SELECT 
        rais.cod_ibge_6,
        rais.year,
        rais.id_employee,
        rais.cbo_02,
        rais.cnae_95,
        rais.edu,
        rais.hours,
        rais.wage,
        rais.contract_type,
        filiado_mun.filiado_id,
        filiado_mun.date_start,
        filiado_mun.date_end,
        filiado_mun.is_filiado
    FROM rais
    LEFT JOIN (SELECT *, 1.0 AS is_filiado FROM filiado_mun) AS filiado_mun
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    "
)

message("generate party dummy and compute partisanship by group")
rais_filiado <- rais_filiado %>%
    generate_year_filiado() %>%
    # filter(
    #     is.na(filiado_id) |
    #     {{is_partisan}}
    # ) %>%
    classify_partisanship() %>%
    mutate(
        is_partisan = if_else(is_partisan == "non_partisan", 0, 1)
    )

rais_filiado_occupation <- rais_filiado %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    ) %>% 
    fix_occupation() %>%
    fix_edu() %>%
    filter(
        !is.na(cbo_group) & !is.na(cbo_group_detail)
    )

message("computing means...")

group_vars <- c(
    "cbo_group", "cbo_group_detail", "contract_type", "edu_category"
)

partisan_summary <- map(
    group_vars,
    ~ compute_summary_stats(
        data = rais_filiado_occupation,
        .group_vars = .,
        .summary_vars = c(is_partisan)
    )
) %>%
    set_names(group_vars)

message("printing out plots")

inputs_to_plot <- list(
    .x = partisan_summary,
    .y = group_vars
) 

plot_partisan <- map2(
    partisan_summary,
    group_vars,
    ~ gg_bar(
        data = .x, 
        x = .y, 
        y = is_partisan_mean
        ) + 
        coord_flip()
    )

message("exporting plots")
path_to_figs <- ifelse(
    isTRUE(debug), 
    "paper/figures/partisanship/sample/", 
    "paper/figures/partisanship/"
)

export_plots <- list(
    filename = sprintf(
        here(path_to_figs, "plot_partisanship_by_%s.pdf"),
        group_vars
        ),
    plot = plot_partisan
)

pwalk(
    export_plots,
    ggsave
)
