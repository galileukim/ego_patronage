# intention: merge partisanship data with rais data
# look at what brollo et al. have done
# not a lot of detail, seems to be simply a fuzzy matching algorithm
# test-run with fastLink

# take into account a few challenges:
# 1) duplicated entries
# 2) duplicated names
# solution: distinct for now
print("set-up")

library(tidyverse)
library(haven)
library(data.table)
library(fastLink)

sample_size <- 1e5
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

extract_year <- function(col) {
    year_col <- str_extract(col, "\\d{4}") %>% 
        as.integer
    
    return(year_col)
}

filiados <- fread(
    "data/filiado_cpf.csv",
    # nrows = sample_size,
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
filiados_clean <- filiados %>%
    select(
        cod_ibge_6,
        elec_title,
        cpf = cpf_candidate,
        name = member_name,
        date_start,
        date_end,
        date_cancel
    ) %>%
    distinct(
        elec_title,
        name,
        .keep_all = T
    ) %>%
    mutate_all(
        as.character
    )

rais_clean <- rais %>%
    mutate_all(
        as.character
    ) %>%
    select(
        cod_ibge_6 = municipio,
        year,
        id_employee,
        cpf,
        name = nome
    ) %>%
    mutate_all(
        as.character
    )

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

# probabilistic matching using fastLink
# block by state year
rais_filiados_no_cpf <- rais_filiados_no_cpf %>%
    split_name() %>%
    mutate(
        state = str_sub(cod_ibge_6, 1, 2)
    )

filiados_clean_with_years <- filiados_clean %>%
    split_name() %>%
    mutate(
        state = str_sub(cod_ibge_6, 1, 2),
        across(
            c(starts_with("date")),
            extract_year,
            .names = "year_{col}"
        ),
        year_termination = pmax(year_date_end, year_date_cancel, na.rm = T) %>%
            replace_na(2019)
    ) %>%
    rename_with(
        ~str_replace(., "year_date", "year"),
        starts_with("year_date")
    )

# subset by year and state
print("perform block merge (state-year) with fastLink")

t <- 2006
s <- 11

rais_merge <- rais_filiados_no_cpf %>% 
    filter(
        year == t,
        state == s
    )

filiados_merge <- filiados_clean_with_years %>%
    filter(
        (year_start < t) & (year_cancel > t) &
        state == s
    )

rais_filiados_link <- fastLink(
    dfA = rais_merge,
    dfB = filiados_merge,
    varnames = c("first_name", "last_name", "middle_name"),
    stringdist.match = c("first_name", "last_name", "middle_name"),
    n.cores = 1
)

matched_rais_filiados <- getMatches(
    dfA = rais_filiados_no_cpf,
    dfB = filiados_clean,
    fl.out = rais_filiados_link,
    combine.dfs = F
)

matched_rais_filiados %>% map(glimpse)

