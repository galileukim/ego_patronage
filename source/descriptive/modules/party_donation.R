# ==============================================================================
# input: party affiliation and campaign donation
# output: compare donations for partisans and non-partisans
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
)

# ==============================================================================
# set up
# ==============================================================================
# campaign data

# only electoral title available for year 2008
campaign_2008 <- fread(
    here("data/raw/tse/receita_candidatos_2008.csv.gz"),
    encoding = "Latin-1"
) %>%
 janitor::clean_names()

campaign <- fread(
    here("data/raw/tse/campaign_local.csv.gz")
)

# filter individual donors and year 2016, mayoral elections
campaign_individual <- campaign %>%
    filter(
        type_source == "recursos de pessoas fisicas" &
        election_year == 2016 &
        position == "prefeito"
    )

filiado <- fread(
    here("data/raw/tse/filiado.csv.gz")
)

bureaucrat_entry <- tbl(rais_con, "rais_bureaucrat_entry")

filiado_bureaucrat <- tbl(rais_con, "filiado_mun") %>%
    select(filiado_id, id_employee, year_start, year_termination) %>%
    filter(year_start <= 2008 & year_termination > 2008) %>%
    inner_join(
        bureaucrat_entry,
        by = "id_employee"
    ) %>%
    collect()

filiado_mun <- fread(
    here("data/clean/id/filiado_with_id_employee_mun.csv.gz")
)

# ==============================================================================
# visualize breakdown of donations by partisan vs. non-partisan
# ==============================================================================
filiado_active <- filiado %>% 
    mutate(
        filiado_id = row_number()
    ) %>%
    fix_year_filiado() %>%
    filter_active_filiado(2008)

campaign_filiado <- campaign_2008 %>% 
    filter(
        ds_titulo == "Recursos de pessoas físicas"
    ) %>%
    transmute(
        electoral_title = as.character(ds_nr_titulo_eleitor) %>%
            str_pad(., 12, "left", "0"),
        ds_titulo,
        vr_receita
    ) %>%
    left_join(
        filiado_active %>% mutate(is_filiado = 1),
        by = c("electoral_title")
    )
    
campaign_filiado_total <- campaign_filiado %>%
    replace_na(
        list(is_filiado = 0)
    ) %>%
    group_by(is_filiado) %>% 
    summarise(
        total_contribution = sum(as.numeric(vr_receita)/1e6, na.rm = TRUE)
    )

campaign_filiado_total %>%
    mutate(
        is_filiado = recode(is_filiado, `0` = "non-partisan", `1` = "partisan")
    ) %>%
    ggplot(
        aes(is_filiado, total_contribution, fill = is_filiado)
    ) +
    geom_col(
        width = 0.5
    ) +
    scale_y_continuous(breaks = scales::pretty_breaks()) +
    xlab("") +
    ylab("Total contribution (million reais)") +
    ggsave(
        here("paper/figures/plot_contribution.pdf")
    )

# ------------------------------------------------------------------------------
# visualize campaign donation distribution
# ------------------------------------------------------------------------------
campaign_individual_aggregate <- campaign_individual %>%
    group_by(cod_ibge_6, cpf_cnpj_donor) %>%
    summarise(
        total_donation = sum(value_receipt),
        total_number_of_donation = n(),
        .groups = "drop"
    )

herfindahl_campaign <- campaign_individual_aggregate %>%
    group_by(cod_ibge_6) %>%
    mutate(
        total_donation_by_mun = sum(total_donation),
        share_donation = total_donation/total_donation_by_mun
    ) %>%
    summarise(
        herfindahl_index = sum(
            share_donation^2
        ),
        total_donation = n()
    )

herfindahl_campaign %>%
    ggplot() +
    geom_histogram(
        aes(herfindahl_index)
    )

# # ==============================================================================
# # test whether we can identify workers
# # ==============================================================================
# # can we make a one-to-one join?
# filiado_mun_first <- filiado_mun[order(date_start)] %>%
#     .[, .SD[1], id_employee]

# # eliminate defective entries
# filiado_mun_first <- filiado_mun_first %>%
#     filter(
#         str_detect(date_start, "^199|^20")
#     )

# # extract only if there is a unique record per date key
# filiado_unique <- filiado %>%
#     select(electoral_title, cod_ibge_6, party, starts_with("date")) %>%
#     filter(
#         str_detect(date_start, "^199|^20")
#     ) %>%
#     group_by(cod_ibge_6, party, date_start, date_end, date_cancel) %>%
#     mutate(n = n()) %>%
#     ungroup() %>%
#     filter(n == 1) %>%
#     select(-n)

# # able to recover around 21% of filiados
# filiado_with_id_employee <- filiado_mun_first %>%
#     merge(filiado_unique, all.x = TRUE)

# look within the ones who are party members
