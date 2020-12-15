# ==============================================================================
# input: sql database of party affiliation
# output: compare membership cycles for workers and non-workers
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais_sql <- tbl(rais_con, "rais")
filiado_sql <- tbl(rais_con, "filiado_mun")

rais_tables <- c(
    partisan = "rais_mun_partisan", 
    non_partisan = "rais_mun_non_partisan"
)

rais_filiado <- map_dfr(
    rais_tables,
    ~ dbGetQuery(rais_con, sprintf("SELECT * FROM %s", .)),
    .id = "is_filiado"
)

# create entries and exits per year
filiado <- filiado_sql %>%
    collect() %>%
    mutate(
        across(starts_with("date"), lubridate::ymd)
    )

filiado_turnover <- filiado %>% 
    pivot_longer(
        c(date_start, date_end),
        names_prefix = "date_",
        names_to = "date"
    ) %>%
    group_by(date, value) %>%
    summarise(n = n())

# verify when data for membership termination begins
filiado_turnover %>%
    ggplot() +
    geom_point(
        aes(x = value, y = n)
    ) +
    geom_vline(xintercept = 
        as.numeric(as.Date(sprintf("%s-01-01", seq(1997, 2013, 4))))
    ) +
    facet_wrap(. ~ date) +
    coord_cartesian(ylim = c(0, 50)) +
    scale_x_date(limit = as.Date(c("2003-01-01", "2019-01-01")))


# compare duration of party membership between employed vs. unemployed
filiado_employed <- filiado_sql %>%
    inner_join(
        rais_sql %>%
            select(),
        by = c("cod_ibge_6", "id_employee")
    ) %>%
    collect()

filiado_employed <- filiado_employed %>%
    