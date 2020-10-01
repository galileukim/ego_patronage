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
rais_filiados <- list()
record_diagnostic <- list()
years <- 2003:2015

for (i in seq_along(years)) {
    init_env <- ls()

    t <- years[i]
    message(sprintf("initializing record linkage for year %s...", t))

    message("reading in files")
    rais_t <- fread(
        rais_id_files[i],
        select = c("cod_ibge_6", "cpf", "name")
    )

    # filter out filiados for year
    filiados_t <- filiados[
        between(t, year_start, year_termination),
        .(cod_ibge_6, name, electoral_title)
    ]
    filiados_t[, year := t]

    message("extrat unique name entries by municipio")
    # extract unique cpf and names by municipio
    rais_t <- unique(rais_t)
    filiados_t <- unique(filiados_t)
    # note there are few duplicates for filiados

    # number of names duplicated by municipality
    # mass of names duplicated by municipality
    message("produce diagnostics for duplicated names")
    diagnostics_duplicate <- list(
        rais = rais_t,
        filiados = filiados_t
    ) %>% 
        map_dfr(diagnose_duplicates, .id = "dataset") %>%
        mutate(year = t)

    # set keys
    setkey(rais_t, cod_ibge_6, name)
    setkey(filiados_t, cod_ibge_6, name)

    message("join rais and filiados data")
    rais_filiados_t <- merge(
        rais_t, filiados_t,
        by = c("cod_ibge_6", "name"),
        all = FALSE
    )

    message("bind to repos")
    record_diagnostic[[i]] <- diagnostics_duplicate
    rais_filiados[[i]] <- rais_filiados_t
}

rais_filiados <- rbindlist(rais_filiados, fill = TRUE) %>%
    select(
        cod_ibge_6, cpf, electoral_title, name
    ) %>% 
    unique()

record_diagnostic <- rbindlist(record_diagnostic, fill = TRUE)

message("write-out data")
rais_filiados %>% 
    fwrite(
        here("data/clean/id/rais_filiado_crosswalk_mun.csv")
    )

record_diagnostic %>%
    fwrite(
        here("data/clean/id/rais_diagnostics_mun.csv")
    )
