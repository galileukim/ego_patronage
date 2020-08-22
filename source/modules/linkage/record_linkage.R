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

debug <- TRUE
sample_size <- ifelse(isTRUE(debug), 1e5, Inf)
between <- data.table::between

source(
    here("source/modules/linkage/preprocess_data.R")
)

# ---------------------------------------------------------------------------- #
# create blocks by kmer (first initial 3 letters of name)
# and state
# deduplicate names within each block to ensure that each name is unique
years <- 2003:2015
for (i in seq_along(years)) {
    t <- years[i]

    rais_t <- fread(rais_id_files[i])

    filiados_t <- filiados[between(t, year_start, year_termination)]
    filiados_t[, year := t]

    records_t <- list(rais = rais_t, filiados = filiados_t)

    records_t <- records_t %>%
        modify(
            ~ mutate(
                .,
                state = str_sub(cod_ibge_6, 1, 2),
                kmer = substr(name, 1, 3)
            ) %>%
                setkey(name)
        )

    records_t <- records_t %>%
        modify(
            ~ filter_group_by_size(., n = 1, name) %>%
                group_nest_dt(year, state, kmer, .key = "nested_data") %>%
                mutate(
                    nested_data = map(nested_data, ~ setkey(., name))
                )
        )

    record_linkage_data <- records_t %>%
        reduce(
            inner_join,
            by = c("year", "state", "kmer"),
            suffix = sprintf("_%s", names(records_t))
        )

    # running into zipth problem
    # create an extra block (using a check)
    # 1) filtering operation upfront to remove high frequency names
    # 2) create a vector of most common names
    # meta analysis: most common names. layering.
    # use functions that work recursively
    # group by, group to group bys.
    # reduce the number of times you do it.
    # be comfortable with fuzzy functions
    # produce metadata on the frequency of these names
    # do probabilistic linkage in the blocks of high frequency names.
    # the zipth distribution has some properties that you can test empirically.
    # zipth stribution (benford distribution)

    

    record_linkage_data[
        ,
        joint_records := map2(
            nested_data_rais,
            nested_data_filiados,
            ~ merge(.x, .y, all = FALSE)
        )
    ]

}

# ---------------------------------------------------------------------------- #


record_linkage_data <- rais_grouped %>%
    inner_join(
        filiados_grouped,
        by = c("state", "kmer")
    )

# record_linkage_data %>% write_rds(here("data/debug/record_linkage.rds"))
record_linkage_data <- read_rds(here("data/debug/record_linkage.rds"))

record_test <- record_linkage_data


# rais_data <- record_test %>%
#     unnest_dt(rais_data, .(year, state, kmer))

# filiados_data <- record_test %>%
#     unnest_dt(filiados_data, .(year, state, kmer))

# rais_data_unique <- rais_data %>%
#     filter_group_by_size(n = 1, name)

# filiados_data_unique <- filiados_data %>%
#     filter_group_by_size(n = 1, name)

# # exact matching
# # note that there are some names that are common: > 500 repeated names
# record_linkage_data <- record_linkage_data %>%
#     mutate(
#         joint_records = map2(
#             rais_data,
#             filiados_data,
#             merge,
#             all = FALSE
#         )
#     )

record_hash <- record_linkage_data %>%
    unnest_dt(joint_records, .(year, state, kmer)) %>%
    select(
        cpf,
        electoral_title,
        name
    )