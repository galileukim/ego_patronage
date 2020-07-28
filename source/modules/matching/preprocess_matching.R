filiados <- fread(
    "data/raw/filiado_cpf.csv",
    nrows = sample_size,
    integer64 = "character"
)

rais <- read_dta(
    paste0(
        path_to_rais,
        "RAIS2006.dta"
    ),
    n_max = sample_size
)

print("prepare data for merge")
# proceed in two parts
# first triage workers with cpf
# merge remainder with fastLink
# note that there are some duplicated names, but they are rare
# also note that the majority of party members have only one affiliation
# the majority of duplicates in RAIS is due to holding several jobs
# final output: unique id and partisanship per year per state
filiados_clean <- filiados %>%
    select(
        cod_ibge_6,
        elec_title,
        cpf = cpf_candidate,
        name = member_name,
        party,
        date_start,
        date_end,
        date_cancel
    ) %>%
    mutate_all(
        as.character
    ) %>%
    create_split_name() %>%
    mutate(
        state = str_sub(cod_ibge_6, 1, 2)
    )

rais_clean <- rais %>%
    mutate_all(
        as.character
    ) %>% 
    mutate_all(
        as.character
    ) %>%
    create_split_name() %>%
    mutate(
        state = str_sub(cod_ibge_6, 1, 2)
    )

print("extracting filiados with cpf")
rais_filiados_with_cpf <- rais_clean %>%
    inner_join(
        filiados_clean,
        by = "cpf"
    )

# triage merged candidates from dataset
rais_filiados_with_cpf_ids <- rais_filiados_with_cpf %>%
    distinct(
        id_employee
    )

rais_filiados_no_cpf <- rais_clean %>%
    anti_join(
        rais_filiados_with_cpf_ids
    )