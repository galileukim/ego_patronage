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
# extract most common names by frequency in party membership data: top 50
common_names <- filiados[
    ,
    .(
        first_name = str_extract(name, "^[a-z]+")
    )
][
    ,
    .(frequency = .N),
    by = first_name
][
    order(-frequency),
    head(.SD, 50)
] %>% setkey(first_name)

common_names[, frequency := NULL]

# create blocks by kmer (first initial 3 letters of name)
# and state
# deduplicate names within each block to ensure that each name is unique
years <- 2003:2015
for (i in seq_along(years)) {
    t <- years[i]

    rais_t <- fread(rais_id_files[i])

    filiados_t <- filiados[between(t, year_start, year_termination)]
    filiados_t[, year := t]

    records_rais_and_filiados_list <- list(
        rais = rais_t,
        filiados = filiados_t
    )

    # create blocks for linkage (state and kmer)
    records_rais_filiados <- records_rais_filiados_list %>%
        modify(
            ~ mutate(
                .,
                state = str_sub(cod_ibge_6, 1, 2),
                kmer = substr(name, 1, 3),
                first_name = str_extract(name, "^[a-z]+")
            ) %>%
            setkey(first_name)
        )

    # records_t_common <- records_t %>%
    #     modify(
    #         ~ .[common_names] # inner join
    #     )

    # records_t_uncommon <- records_t %>%
    #     modify(
    #         ~ .[!common_names] # anti join
    #     )

    # extract unique names and nest data
    # need to extract common names before grouping
    # questions:
    # 1) what is the proportion of names with n > 1?
    # 2) what is the mass of these duplicated names?
    records_diagnostics <- records_rais_filiados %>%
        map_dfr(
            ~ .[, .(n = .N), by = name] %>% # create tally of obs by name
                summarise(
                    n_records = n(),
                    prop_names = mean(n > 1), # prop of dup. names
                    density_names = sum(n[n > 1])/sum(n) # mass of dup. names
                ),
            .id = "dataset"
        )

    records_rais_filiados_nested <- records_rais_filiados %>%
        modify(
            # ~ filter_group_by_size(., n = 1, name) %>%
                ~ filter_group_by_size(., name) %>%
                    group_nest_dt(
                    .,
                    year, state, kmer, .key = "nested_data"
                ) %>%
                mutate(
                    nested_data = map(nested_data, ~ setkey(., name))
                )
        )

    # join rais and filiado blocks
    record_linkage_data <- records_rais_filiados_nested %>%
        reduce(
            inner_join,
            by = c("year", "state", "kmer"),
            suffix = sprintf("_%s", names(records_t))
        )

    # join rais and filiado names through exact match
    # exclude all empty blocks
    # questions:
    # 1) how many rais workers can we match?
    # 2) how many filiados can we match?
    record_linkage_data <- record_linkage_data %>%
        transmute(
            linked_records = map2(
            nested_data_rais,
            nested_data_filiados,
            ~ merge(.x, .y, all = FALSE)
            ),
            n_records = map_dbl(linked_records, nrow)
        ) %>%
        filter(
            n_records > 0
        ) %>%
        select(-n_records) %>%
        unnest(
            cols = c(linked_records)
        )

    record_link <- record_linkage_data %>%
        select(year, state, name, cpf, electoral_title)
    
    n_match <- nrow(record_link)

    records_diagnostics <- records_diagnostics %>%
        mutate(
            prop_matched = n_match/n_records
        )
}

#notes:
# running into zipth problem
# create an extra block (using a check)
# 1) filtering operation upfront to remove high frequency names
# 2) create a vector of most common names
# meta analysis: most common names. layering.
# use functions that work recursively
# reduce the number of iterations
# be comfortable with fuzzy functions
# produce metadata on the frequency of these names
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