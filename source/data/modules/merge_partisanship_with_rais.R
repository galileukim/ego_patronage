# intention: merge partisanship data with rais data
# look at what brollo et al. have done
# not a lot of detail, seems to be simply a fuzzy matching algorithm
# test-run with fastLink

# take into account a few challenges:
# 1) duplicated entries
# 2) duplicated names
# solution: distinct for now
library(tidyverse)
library(haven)
library(data.table)
library(fastLink)

path_to_rais <- "/home/BRDATA/RAIS/"

# aux funs
split_name <- function(data) {
    data %>%
        mutate(
            name = str_to_lower(name),
            first_name = str_extract(name, "^[a-z]+"),
            last_name = str_extract(name, "[a-z]+$"),
            middle_name = str_extract(name, "(?<=\\s)[a-z\\s]+(?=\\s)")
        )
}

filiados <- fread(
    "data/filiado_cpf.csv",
    nrows = 1e4,
    integer64 = "character"
)

rais <- read_dta(
    paste0(
        path_to_rais,
        "RAIS2006.dta"
    ),
    n_max = 1e4
)

print("prepare data for merge")
# proceed in two parts
# first triage workers with cpf
# merge remainder with fastLink
# note that there are some duplicated names, but they are rare
# the majority of duplicates is attributable to holding several jobs
filiados_merge <- filiados %>%
    select(
        cod_ibge_6,
        elec_title,
        cpf = cpf_candidate,
        name = member_name
    ) %>%
    distinct(
        elec_title,
        name,
        .keep_all = T
    ) %>%
    mutate_all(
        as.character
    )

rais_merge <- rais %>%
    mutate_all(
        as.character
    ) %>%
    select(
        cod_ibge_6 = municipio,
        id_employee,
        cpf,
        name = nome
    ) %>%
    distinct(
        id_employee,
        .keep_all = T
    ) %>%
    mutate_all(
        as.character
    )

rais_filiados_with_cpf <- rais_merge %>%
    inner_join(
        filiados_merge,
        by = "cpf"
    )

# triage merged candidates from dataset
rais_filiados_with_cpf_ids <- rais_filiados_with_cpf %>%
    distinct(
        id_employee
    )

rais_filiados_no_cpf <- rais_merge %>%
    anti_join(
        rais_filiados_with_cpf_ids
    )

# probabilistic matching using fastLink
rais_filiados_no_cpf <- rais_filiados_no_cpf %>%
    split_name()

filiados_merge <- filiados_merge %>%
    split_name()

rais_filiados_link <- fastLink(
    dfA = rais_filiados_no_cpf,
    dfB = filiados_merge,
    varnames = c("first_name", "last_name", "middle_name"),
    stringdist.match = c("first_name", "last_name", "middle_name"),
    n.cores = 1
)

matched_rais_filiados <- getMatches(
    dfA = rais_filiados_no_cpf,
    dfB = filiados_merge,
    fl.out = rais_filiados_link,
    combine.dfs = F
)
