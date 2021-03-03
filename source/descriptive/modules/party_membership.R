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
# filiado_unemployed <- filiado_sql %>%
#     anti_join(
#         rais_sql %>%
#             select(cod_ibge_6, id_employee),
#         by = c("cod_ibge_6", "id_employee")
#     ) %>%
#     collect()

# filiado_employed <- filiado_sql %>%
#     inner_join(
#         rais_sql %>%
#             select(cod_ibge_6, id_employee),
#         by = c("cod_ibge_6", "id_employee")
#     ) %>%
#     collect() %>%
#     distinct(filiado_id, .keep_all = TRUE)

# compare duration of party membership between employed vs. unemployed
# filiado_turnover <- list(
#     filiado_employed,
#     filiado_unemployed
# ) %>%
#     set_names(c("employed", "unemployed")) %>%
#     map_dfr(
#         . %>%
#             convert_to_date() %>%
#             pivot_longer(
#                 c(date_start, date_end),
#                 names_prefix = "date_",
#                 names_to = "date"
#             ) %>%
#             count(date, value),
#         .id = "is_employed"
#     )

# first determine how many party members there are per annum
filiado_date <- filiado %>% 
    transmute(
        date_start, 
        date_end = coalesce(date_end, date_cancel)
     ) %>%
    fix_year_filiado() %>%
    filter(
        between(date_start, 1997, 2018)
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

filiado_active_year %>% 
    filter(
        between(year, 1997, 2018)
    ) %>%
    gg_point_line(
        aes(year, n)
    ) +
    labs(
        x = "Year",
        y = "Total active party members"
    ) +
    ggsave(
        here("paper/figures/partisanship/plot_partisan_by_year.pdf")
    )
# compare membership spells
