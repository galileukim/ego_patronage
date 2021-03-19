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

# first determine how many party members there are per annum
filiado_date <- filiado %>%
    transmute(
        party,
        region = str_sub(cod_ibge_6, 1, 1),
        date_start, 
        date_end = coalesce(date_end, date_cancel) %>%
            if_else(
                . == "", "2019", .
            )
     ) %>%
    fix_year_filiado() %>%
    filter(
        between(date_start, 1997, 2019) &
        between(date_end, 1997, 2019)
    )

range_record <- range(filiado_date$date_start)
range_record <- range_record[1]:range_record[2]

filiado_active_year <- map_dfr(
    range_record,
    ~ filter_active_filiado(year = .x, data = filiado_date) %>%
        nrow() %>%
        tibble(
            n = .
        ) %>%
        mutate(year = .x)
)

filiado_active_year_by_region <- map_dfr(
    range_record,
    ~ filter_active_filiado(year = .x, data = filiado_date) %>%
        count(region) %>%
        mutate(year = .x)
)

filiado_active_year %>% 
    filter(
        between(year, 1997, 2018)
    ) %>%
    gg_point_line(
        aes(year, n)
    ) +
    labs(
        x = "Year",
        y = "Total active party members (millions)"
    ) +
    geom_vline(
        xintercept = seq(2002, 2018, 4),
        linetype = "dashed"
    ) +
    scale_y_continuous(
        breaks = c(0, 5e6, 1e7),
        labels = c(0, 5, 10)
    ) +
    ggsave(
        here("paper/figures/partisanship/plot_partisan_by_year.pdf")
    )

# growth in membership by state
filiado_active_year_by_region %>% 
    filter(
        between(year, 1997, 2018)
    ) %>%
    mutate(
        region = recode(
            region,
            `1` = "North",
            `2` = "Northeast",
            `3` = "Southeast",
            `4` = "South",
            `5` = "Midwest"
        )
    ) %>%
    gg_point_line(
        aes(year, n, color = region)
    ) +
    labs(
        x = "Year",
        y = "Total active party members (millions)"
    ) +
    geom_vline(
        xintercept = seq(2002, 2018, 4),
        linetype = "dashed"
    ) +
    scale_y_continuous(
        breaks = c(0, 5e6, 1e7),
        labels = c(0, 5, 10)
    ) +
    ggsave(
        here("paper/figures/partisanship/plot_partisan_by_region.pdf")
    )

# duration spells
filiado_date_spell <- filiado %>%
    transmute(
        date_start,
        date_end = coalesce(date_end, date_cancel) %>%
            if_else(
                . == "",
                date_record_extraction, .
            ),
        duration = lubridate::interval(
            lubridate::as_date(date_start), lubridate::as_date(date_end)
        ) / lubridate::years(1)
    )

# what we ultimately want is number of party members by year
filiado_date_spell <- filiado_date_spell %>%
    mutate(
        across(
            starts_with("date"), 
            ~ str_extract(., "^\\d{4}") %>%
                as.integer,
            .names = "year_{col}"
        )
    ) %>%
    filter(
        year_date_start >= 1985 &
        year_date_start <= 2019 & 
        year_date_end <= 2019
    )

filiado_date_spell %>%
    filter(year_date_start >= 1997) %>%
    filter(duration > 0) %>%
    ggplot() +
    geom_histogram(
        aes(duration), binwidth = 1
    ) +
    scale_y_continuous(
        breaks = c(0, 5e6, 1e7),
        labels = c(0, 5, 10)
    ) +
    ggsave(
        here::here("paper/figures/partisanship/plot_partisan_spell.pdf")
    )
