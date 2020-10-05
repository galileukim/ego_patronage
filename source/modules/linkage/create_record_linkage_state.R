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
rais_filiados <- list()nrecord_diagnostic <- list()
years <- 2003:2015

for (i in seq_along(years)) {
    init_env <- ls()

    t <- years[i]
    message(sprintf("initializing record linkage for year %s...", t))

    message("reading in files")
    rais_t <- fread(
        rais_id_files[i],
        select = c("cod_ibge_6", "cpf", "name")
    ) %>%
        mutate(
            state = str_sub(cod_ibge_6, 1, 2)
        ) %>%
        select(-cod_ibge_6)

    # filter out filiados for year
    filiados_t <- filiados[
        between(t, year_start, year_termination),
        .(state = str_sub(cod_ibge_6, 1, 2), name, electoral_title)
    ]
    filiados_t[, year := t]

    # number of names duplicated by municipality
    # mass of names duplicated by municipality
    message("produce diagnostics for duplicated names")
    diagnostics_duplicate <- list(
        rais = rais_t,
        filiados = filiados_t
    ) %>%
        map_dfr(
             ~ diagnose_duplicates(., group = .(state, name)),
             .id = "dataset"
             ) %>%
        mutate(year = t)

    message("extract only names that are unique by municipio")
    rais_t_dedupe <- rais_t %>%
        remove_duplicate_by_group(
            group = .(state, name)
        )

    filiados_t_dedupe <- filiados_t %>%
        remove_duplicate_by_group(
            group = .(state, name)
        )

    # set keys
    setkey(rais_t_dedupe, state, name)
    setkey(filiados_t_dedupe, state, name)

    message("join rais and filiados data")
    rais_filiados_t <- merge(
        rais_t_dedupe, filiados_t_dedupe,
        by = c("state", "name"),
        all = FALSE
    )

    message("bind to repos")
    record_diagnostic[[i]] <- diagnostics_duplicate %>%
        mutate(
            prop_matched = nrow(rais_filiados_t)/total_records
        )
        
    rais_filiados[[i]] <- rais_filiados_t
}

rais_filiados <- rbindlist(rais_filiados, fill = TRUE) %>%
    select(
        state, cpf, electoral_title, name
    ) %>%
    unique()

record_diagnostic <- rbindlist(record_diagnostic, fill = TRUE)

message("write-out data")
rais_filiados %>%
    fwrite(
        here("data/clean/id/rais_filiado_crosswalk_state.csv")
    )

record_diagnostic %>%
    fwrite(
        here("data/clean/id/rais_diagnostics_state.csv")
    )

message("record linkage by state complete!")