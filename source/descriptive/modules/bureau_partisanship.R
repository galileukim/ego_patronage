# ==============================================================================
# input: sql database of party affiliation
# output: assess extent to which there is party dominance
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

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

rais_filiado <- rais_filiado %>%
    generate_year_filiado() %>%
    filter(
        is.na(filiado_id) |
        {{is_partisan}}
    ) %>%
    mutate(
        is_partisan = if_else(
            is.na(filiado_id), 0L, 1L
        )
    )

rais_filiado_occupation <- rais_filiado %>%
    mutate(
        occupation = str_sub(cbo_02, 1, 1)
    ) %>% 
    fix_occupation() 

group_vars <- c(
    "cbo_group", "cbo_group_detail", "contract_type", "edu", "hours"
) 

partisan_summary <- map(
    group_vars,
    ~ compute_mean(
        data = rais_filiado_occupation,
        .group_vars = .,
        .summary_vars = c(is_partisan)
    ) %>%
    arrange(desc(mean_is_partisan))
) %>%
    set_names(group_vars)

inputs_to_plot <- list(
    data = partisan_summary,
    x = group_vars
) 

pmap(
    inputs_to_plot,
    gg_bar, y = mean_is_partisan
    )
