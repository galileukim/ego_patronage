# input: a) filiado data
# b) rais data
# output: table containing the linkage between electoral_title and cpf
# notes: create a unique identifier that tracks row number and file id
# modularize each function to perform the linkage within each block
# nest, nest, nest!
source(
    here::here("source/modules/setup_preprocess.R")
)

source(
    here("source/utils/record_linkage.R")
)

debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e6, Inf)

source(
    here("source/modules/linkage/preprocess_data.R")
)

# ---------------------------------------------------------------------------- #
# create blocks by kmer (first initial 3 letters of name)
# and state
# deduplicate names within each block to ensure that each name is unique
rais_grouped <- rais_t %>%
    group_nest_dt(year, state, kmer, .key = "rais_data") %>%
    mutate(
        rais_data = map(rais_data, ~setkey(., name))
    )

filiados_grouped <- filiados_t %>%
    group_nest_dt(state, kmer, .key = "filiados_data") %>%
    mutate(
        filiados_data = map(filiados_data, ~setkey(., name))
    )

record_linkage_data <- rais_grouped %>%
    inner_join(
        filiados_grouped,
        by = c("state", "kmer")
    )

# record_linkage_data %>% write_rds(here("data/debug/record_linkage.rds"))
record_linkage_data <- read_rds(here("data/debug/record_linkage.rds"))

record_test <- record_linkage_data[1]

record_test[
    ,
    .(
        rais_data_unique = map(rais_data, ~ filter_group_by_size(., n = 1, name)),
        filiados_data_unique = map(filiados_data, ~ filter_group_by_size(., n = 1, name))
    ),
    by = .(year, state, kmer)
]

rais_data <- record_test %>%
    unnest_dt(rais_data, .(year, state, kmer))

filiados_data <- record_test %>%
    unnest_dt(filiados_data, .(year, state, kmer))

rais_data_unique <- rais_data %>%
    filter_group_by_size(n = 1, name)

filiados_data_unique <- filiados_data %>%
    filter_group_by_size(n = 1, name)

merge(rais_data_unique, filiados_data_unique, all = FALSE)


# exact matching
# note that there are some names that are common: > 500 repeated names
record_linkage_data <- record_linkage_data %>%
    mutate(
        joint_records = map2(
            rais_data,
            filiados_data, 
            merge,
            all = FALSE
        )
    )

record_hash <- record_linkage_data %>%
    unnest_dt(joint_records, .(year, state, kmer)) %>%
    select(
        cpf,
        electoral_title,
        name
    )

