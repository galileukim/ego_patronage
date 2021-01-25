# ==============================================================================
# input: rais municipal sql tables
# output: descriptive statistics on partisan vs. non-partisan
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
)

plot_map_partisanship <- 
# ---------------------------------------------------------------------------- #
message("import map")

map_br <- st_read(
    here("data/clean/maps"),
    layer = "municipio"
) %>%
    mutate(
        cod_ibge_6 = MUNICÍPI0 %>%
            as.character() %>%
            as.integer()
    )

# ---------------------------------------------------------------------------- #
message("importing data")
party_dominance <- read_data(
    "clean",
    "summary",
    "partisan_dominance.rds"
)

party_dominance_mun <- party_dominance %>%
    pluck("occupation") %>%
    filter(year == max(year)) %>%
    group_by(cod_ibge_6) %>%
    summarise(
        partisanship = weighted.mean(partisanship, n),
        .groups = "drop"
    )

party_dominance_disaggregated <- party_dominance %>%
    map(
        ~ filter(., year == max(year))
    )

# fix education categories
party_dominance_disaggregated <- party_dominance_disaggregated %>%
    modify_at(
        "education",
    ~mutate(
        ., 
        edu_category = if_else(
            str_detect(edu_category, "lower|middle"),
            "lower-middle school",
            edu_category
        )
    ) %>%
    group_by(cod_ibge_6, edu_category) %>%
    summarise(
        partisanship = weighted.mean(partisanship, n),
        .groups = "drop"
    )
    )

# ---------------------------------------------------------------------------- #
message("join data")
map_br_partisan <- map_br %>%
    left_join(
        party_dominance_mun,
        by = "cod_ibge_6"
    )

map_br_disaggregated <- party_dominance_disaggregated %>%
    map(
        ~ left_join(
            map_br,
            .,
            by = "cod_ibge_6"
        )
    )

map_partisanship <- map_br_partisan %>%
    gg_map(partisanship)

map_occupation <- map_br_disaggregated %>%
    pluck("occupation") %>%
    # filter(cbo_group_detail %in% c("executive")) %>%
    filter(
        cbo_group_detail %in%
            c("executive", "administration", "services", "liberal arts")
    ) %>%
    gg_map(partisanship) +
    facet_wrap(. ~ cbo_group_detail)

map_contract <- map_br_disaggregated %>%
    pluck("contract") %>%
    filter(contract_type %in% c("regular", "career")) %>%
    gg_map(partisanship) +
    facet_wrap(. ~ contract_type)

map_education <- map_br_disaggregated %>%
    pluck("education") %>%
    gg_map(partisanship) +
    facet_wrap(. ~ edu_category)

export_maps <- list(
    plot = list(map_partisanship, map_occupation, map_contract, map_education),
    filename = sprintf(
        here("paper/figures/maps/%s.png"), c("pooled", names(party_dominance))
        )
)

pwalk(
    export_maps,
    ggsave,
    height = 5,
    width = 5
)
