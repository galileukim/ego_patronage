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
# only electoral title available for year 2008
campaign <- fread(
    here("data/raw/tse/receita_candidatos_2008.csv.gz"),
    encoding = "Latin-1"
) %>%
 janitor::clean_names()

filiado <- fread(
    here("data/raw/tse/filiado.csv.gz")
)

# ==============================================================================
# visualize breakdown of donations by partisan vs. non-partisan
# ==============================================================================
filiado_active <- filiado %>% 
    fix_year_filiado() %>%
    filter_active_filiado(2008)

campaign_filiado <- campaign %>% 
    filter(
        ds_titulo == "Recursos de pessoas físicas"
    ) %>%
    transmute(
        ds_nr_titulo_eleitor = as.character(ds_nr_titulo_eleitor) %>%
            str_pad(., 12, "left", "0"),
        ds_titulo,
        vr_receita
    ) %>%
    left_join(
        filiado_active %>% mutate(is_filiado = 1),
        by = c("ds_nr_titulo_eleitor" = "electoral_title")
    )
    
campaign_filiado %>%
    group_by(is_filiado) %>% 
    summarise(
        sum(as.numeric(vr_receita), na.rm = TRUE)
    )
