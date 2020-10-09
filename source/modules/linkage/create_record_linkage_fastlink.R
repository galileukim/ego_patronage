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

debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e5, Inf)
between <- data.table::between

source(
    here("source/modules/linkage/preprocess_data.R")
)

# ---------------------------------------------------------------------------- #
rais_filiados <- list()
rais_filiados_diagnostic <- list()
years <- 2003:2005

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

    message("extract only names that are unique by municipio")
    rais_t_dedupe <- rais_t %>%
        remove_duplicate_by_group(
            group = .(cod_ibge_6, name)
        )

    filiados_t_dedupe <- filiados_t %>%
        remove_duplicate_by_group(
            group = .(cod_ibge_6, name)
        )

    # create name vars and nest by municipality
    rais_t_nested <- rais_t %>%
        create_split_name() %>%
        nest(rais = -cod_ibge_6)

    filiados_t_nested <- filiados_t_dedupe %>%
        create_split_name() %>%
        nest(filiados = -cod_ibge_6)

    rais_filiados_t <- rais_t_nested %>%
        inner_join(
            filiados_t_nested,
            by = "cod_ibge_6"
        )

    rais_filiados_linked <- rais_filiados_t %>%
        mutate(
            link = map2(
                rais,
                filiados,
                ~ match_fastLink(
                    .x, .y
                )
            ),
            n_matches = nrow_data(link)
        ) %>%
        filter(
            n_matches > 0
        )

    # produce diagnostics
    linkage_diagnostics <- rais_filiados_linked %>%
        mutate(
            rais = nrow_data(rais),
            filiados = nrow_data(filiados)
        ) %>%
        summarise(
            across(
                c(rais, filiados),
                ~ round(n_matches/., 2),
                .names = "prop_{.col}_matched"
            )
        )

    # unnest hash table
    rais_filiados_linked <- rais_filiados_linked %>%
        select(link) %>%
        unnest(
            col = c(link)
        )

    message("write out data")
    rais_filiados[[i]] <- rais_filiados_linked
    rais_filiados_diagnostic[[i]] <- linkage_diagnostics
}

rais_filiados <- rbindlist(rais_filiados, fill = TRUE) %>%
    select(
        cpf, electoral_title, name
    ) %>%
    unique()

rais_filiados_diagnostic <- rbindlist(rais_filiados_diagnostic, fill = TRUE)

message("write-out data")
rais_filiados %>%
    fwrite(
        here("data/clean/id/rais_filiado_crosswalk_fastlink.csv")
    )

rais_filiados_diagnostic %>%
    fwrite(
        here("data/clean/id/rais_diagnostics_fastlink.csv")
    )