# input: a) filiado data
# b) rais data
# output: table with probabilistic linkage between electoral_title and cpf
# notes: create a unique identifier that tracks row number and file id
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
# create blocks by state and year
# deduplicate names within each block to ensure that each name is unique
record_hash <- list()
record_diagnostic <- list()

years <- 2003:2015
i <- 1
init_env <- ls()

t <- years[i]
message("initializing record linkage for year ", t, "...")

message("reading in files")
rais_t <- fread(rais_id_files[i])

filiados_t <- filiados[between(t, year_start, year_termination)]
filiados_t[, year := t]

record_rais_filiados_list <- list(
        rais = rais_t,
        filiados = filiados_t
    )

    message("creating blocks for linkage")
    # create blocks for linkage (state and kmer)
    record_rais_filiados <- record_rais_filiados_list %>%
        modify(
            ~ mutate(
                .,
                state = str_sub(cod_ibge_6, 1, 2),
                first_name = str_extract(name, "^[a-z]+")
            ) %>%
            setkey(first_name)
        )