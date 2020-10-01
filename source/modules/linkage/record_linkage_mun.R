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
sample_size <- ifelse(isTRUE(debug), 1e5, Inf)
between <- data.table::between

source(
    here("source/modules/linkage/preprocess_data.R")
)

# ---------------------------------------------------------------------------- #
# extract most common names by frequency in party membership data: top 50
# common_names <- filiados[
#     ,kl
#     .(
#         first_name = str_extract(name, "^[a-z]+")
#     )
# ][
#     ,
#     .(frequency = .N),
#     by = first_name
# ][
#     order(-frequency),
#     head(.SD, 50)
# ] %>% setkey(first_name)

# common_names[, frequency := NULL]

# create blocks by kmer (first initial 3 letters of name)
# and cod_ibge_6
# deduplicate names within each block to ensure that each name is unique
record_hash <- list()
record_diagnostic <- list()
years <- 2003:2015

for (i in seq_along(years)) {
    init_env <- ls()

    t <- years[i]
    cat(sprintf("initializing record linkage for year %s...", t))

    cat("reading in files")
    rais_t <- fread(rais_id_files[i])

    filiados_t <- filiados[between(t, year_start, year_termination)]
    filiados_t[, year := t]

    record_rais_filiados_list <- list(
        rais = rais_t,
        filiados = filiados_t
    )

    cat("creating blocks for linkage")
    # create blocks for linkage (cod_ibge_6 and kmer)
    record_rais_filiados <- record_rais_filiados_list %>%
        modify(
            ~ mutate(
                .,
                kmer = substr(name, 1, 3),
                first_name = str_extract(name, "^[a-z]+")
            ) %>%
                setkey(first_name)
        )

    cat("nest and join rais and filiados data")
    record_rais_filiados_nested <- record_rais_filiados %>%
        modify(
            # ~ filter_group_by_size(., n = 1, name) %>%
            ~ filter_group_by_size(., name) %>%
                group_nest_dt(
                    .,
                    year, cod_ibge_6, kmer,
                    .key = "nested_data"
                ) %>%
                mutate(
                    nested_data = map(nested_data, ~ setkey(., name))
                )
        )

    # join rais and filiado blocks
    record_linkage_data <- record_rais_filiados_nested %>%
        reduce(
            inner_join,
            by = c("year", "cod_ibge_6", "kmer"),
            suffix = sprintf("_%s", names(record_rais_filiados))
        )

    # join rais and filiado names through exact match
    # exclude all empty blocks
    # questions:
    # 1) how many rais workers can we match?
    # 2) how many filiados can we match?
    cat("join through exact match")
    record_linkage_data <- record_linkage_data %>%
        transmute(
            year,
            cod_ibge_6,
            linked_record = map2(
                nested_data_rais,
                nested_data_filiados,
                ~ merge(.x, .y, all = FALSE)
            ),
            n_record = map_dbl(linked_record, nrow)
        ) %>%
        filter(
            n_record > 0
        ) %>%
        select(-n_record) %>%
        unnest(
            cols = c(linked_record)
        )

    record_linkage <- record_linkage_data %>%
        select(year, cod_ibge_6, name, cpf, electoral_title)

    # final diagnostic: proportion of matches
    n_match <- nrow(record_linkage)

    record_diagnostics <- record_rais_filiados %>%
        map_dfr(
            ~ .[, .(n = .N), by = name] %>% # create tally of obs by name
                summarise(
                    n_record = n()
                ),
            .id = "dataset"
        )

    record_diagnostics <- record_diagnostics %>%
        mutate(
            prop_matched = n_match / n_record,
            year = t
        )

    cat("write out hash table and diagnostics.")
    record_hash[[i]] <- record_linkage %>%
        setkey(name, cpf, electoral_title) %>%
        unique(
            record_hash,
            by = c("name", "cpf", "electoral_title")
        )

    record_diagnostic[[i]] <- record_diagnostics

    reset_env(init_env)

    cat("record_linkage complete!\n # ----- # ")
}

record_hash %>%
    rbindlist(fill = TRUE) %>%
    fwrite(
        here("data/clean/id/rais_filiado_crosswalk_mun.csv")
    )

record_diagnostic %>%
    rbindlist(fill = TRUE) %>%
    fwrite(
        here("data/clean/id/rais_filiado_linkage_diagnostics_mun.csv")
    )