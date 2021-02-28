# ==============================================================================
# input: sql database of party affiliation
# output: compare membership cycles for workers and non-workers
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
)

# ==============================================================================
# generate descriptive statistics of party members
# ==============================================================================
rais_sql <- tbl(rais_con, "rais") %>%
    filter_municipal()
    
filiado_sql <- tbl(rais_con, "filiado_mun")

filiado <- fread(
    here("data/raw/tse/filiado.csv.gz")
)

# create entries and exits per year
# segmented by employment
filiado_unemployed <- filiado_sql %>%
    anti_join(
        rais_sql %>%
            select(cod_ibge_6, id_employee),
        by = c("cod_ibge_6", "id_employee")
    ) %>%
    collect()

filiado_employed <- filiado_sql %>%
    inner_join(
        rais_sql %>%
            select(cod_ibge_6, id_employee),
        by = c("cod_ibge_6", "id_employee")
    ) %>%
    collect() %>%
    distinct(filiado_id, .keep_all = TRUE)

# compare duration of party membership between employed vs. unemployed
filiado_turnover <- list(
    filiado_employed,
    filiado_unemployed
) %>%
    set_names(c("employed", "unemployed")) %>%
    map_dfr(
        . %>%
            convert_to_date() %>%
            pivot_longer(
                c(date_start, date_end),
                names_prefix = "date_",
                names_to = "date"
            ) %>%
            count(date, value),
        .id = "is_employed"
    )

# verify when data for membership termination begins
filiado_turnover %>%
    gg_point(
        aes(x = value, y = n),
        size = 2
    ) +
    plot_mandate_year() +
    facet_wrap(. ~ date + is_employed) +
    coord_cartesian(ylim = c(0, 50)) +
    scale_x_date(limit = as.Date(c("2003-01-01", "2019-01-01")))

# compare membership spells
