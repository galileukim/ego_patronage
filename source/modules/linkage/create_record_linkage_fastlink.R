# input: a) filiado data
# b) rais data
# output: table containing the linkage between electoral_title and cpf
# notes: we use fastLink to perform the merge
library(fastLink)

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

    # number of names duplicated by municipality
    # mass of names duplicated by municipality
    message("produce diagnostics for duplicated names")
    diagnostics_duplicate <- list(
        rais = rais_t,
        filiados = filiados_t
    ) %>%
        map_dfr(
            ~ diagnose_duplicates(., group = .(cod_ibge_6, name)),
            .id = "dataset"
        ) %>%
        mutate(year = t)

    message("extract only names that are unique by municipio")
    rais_t_dedupe <- rais_t %>%
        remove_duplicate_by_group(
            group = .(cod_ibge_6, name)
        )

    filiados_t_dedupe <- filiados_t %>%
        remove_duplicate_by_group(
            group = .(cod_ibge_6, name)
        )

    # create name vars and nest
    rais_t_nested <- rais_t %>%
        create_split_name() %>%
        nest(rais = -cod_ibge_6)

    filiados_t_nested <- filiados_t_dedupe %>%
        create_split_name() %>%
        nest(filiados = -cod_ibge_6)

    rais_filiados <- rais_t_nested %>%
        inner_join(
            filiados_t_nested,
            by = "cod_ibge_6"
        )

    rais_filiados_linked <- rais_filiados %>%
        mutate(
            link = map2(
                rais,
                filiados,
                ~ fastLink(
                    dfA = .x,
                    dfB = .y,
                    varnames = c("first_name", "middle_name", "last_name"),
                    # partial.match = c("first_name", "middle_name", "last_name"),
                    dedupe.matches = TRUE,
                    n.cores = 1
                )
            )
        )

}