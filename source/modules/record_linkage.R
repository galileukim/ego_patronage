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

rais_id_path <- here("data/clean/id/rais_hash")

rais_id_files <- list.files(
    rais_id_path,
    full.names = T
)

# ---------------------------------------------------------------------------- #
# repeat procedure for each year
filiados <- fread(
    here("data/clean/id/filiado_id_without_cpf.csv.gz"),
    select = c(
        "state", "name", "electoral_title", "year_start", "year_termination"
    )
)

# note that thisdata is still incomplete
filiados <- filiados %>%
    setkey(
        year_start, year_termination
    )

rais_t <- fread(
    rais_id_files[1]
)

filiados_t <- filiados[data.table::between(t, year_start, year_termination)]

# ---------------------------------------------------------------------------- #
rais_t <- rais_t %>%
    mutate(
        state = str_sub(cod_ibge_6, 1, 2),
        kmer = substr(name, 1, 3)
    )

filiados_t <- filiados_t %>%
    mutate(
        state = as.character(state),
        kmer = substr(name, 1, 3)
    )

# ---------------------------------------------------------------------------- #
# create blocks
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

# exact matching
record_linkage_data <- record_linkage_data %>%
    mutate(
        joint_records = map2(
            rais_data,
            filiados_data, 
            ~ merge(.x, .y, all = FALSE)
        )
    )

# rais_data <- record_test %>%
#     unnest_dt(rais_data, .(year, state, kmer))

# filiados_data <- record_test %>%
#     unnest_dt(filiados_data, .(year, state, kmer))

record_hash <- record_linkage_data %>%
    unnest_dt(joint_records, .(year, state, kmer)) %>%
    select(
        cpf,
        electoral_title,
        name
    )

