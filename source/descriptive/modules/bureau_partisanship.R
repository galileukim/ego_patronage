# ==============================================================================
# input: sql database of party affiliation
# output: assess extent to which there is party dominance
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
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
        rais.work_experience,
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

rais_filiado <- tbl(rais_con, "rais") %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    ) %>%
    mutate(
        cbo_group_detail = case_when(
        occupation == 0 ~ "army",
        `1` = "executive",
        `2` = "liberal arts",
        `3` = "engineer",
        `4` = "administration",
        `5` = "services",
        `6` = "agriculture",
        `7` = "industry",
        `8` = "industry",
        `9` = "maintenance"
      )
    )

dbGetQuery(
    "
    SELECT
    edu,
    AVG(COALESCE(is_filiado, 0)) AS party_dominance
    FROM rais
    LEFT JOIN (SELECT *, 1.0 AS is_filiado FROM filiado_mun) AS filiado_mun
    ON rais.cod_ibge_6 = filiado_mun.cod_ibge_6 AND
    rais.id_employee = filiado_mun.id_employee
    GROUP BY edu
    "   
)

message("generate party dummy and compute partisanship by group")
rais_filiado_occupation <- rais_filiado %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    ) %>% 
    fix_occupation() %>%
    fix_edu() %>%
    filter(
        !is.na(cbo_group) & !is.na(cbo_group_detail)
    )

# ==============================================================================
# compute party dominance across different categories
# ==============================================================================
message("computing dominance...")

group_vars <- c(
    "cbo_group", "cbo_group_detail", "contract_type", "edu_category"
)

partisan_summary <- map(
    group_vars,
    ~ compute_summary_stats(
        data = rais_filiado_occupation,
        .group_vars = all_of(.),
        .summary_vars = c(is_partisan)
    )
) %>%
    set_names(group_vars)

message("printing out plots")

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

# ==============================================================================
# exporting plots and data
# ==============================================================================
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
    save_plot
)

partisan_summary %>%
    write_data(
        dir = "data/clean/summary_tables/",
        filename = "partisan_dominance.rds"
    )
