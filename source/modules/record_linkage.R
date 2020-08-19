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

rais_id_path <- here("data/clean/id/rais_hash")

rais_id_files <- list.files(
    rais_id_path,
    full.names = T
)

# ---------------------------------------------------------------------------- #
# repeat procedure for each year
rais <- fread(
    rais_id_files[1]
)

filiados <- fread(
    here("data/clean/id/filiado_id_without_cpf.csv.gz"),
    select = c("state", "name", "year_start", "year_termination")
)

# ---------------------------------------------------------------------------- #
rais <- rais %>%
    mutate(
        state = str_sub(cod_ibge_6, 1, 2),
        kmer = substr(name, 1, 3)
    )

filiados <- filiados %>%
    mutate(
        state = as.character(state),
        kmer = substr(name, 1, 3)
    )

# ---------------------------------------------------------------------------- #
# create blocks

rais_grouped <- rais %>%
    group_nest(
        year, state, kmer
    )

rais_grouped <- rais[
    ,
    .(data = list(.SD)),
    by = .(year, state, kmer)
]

filiados_grouped <- filiados[
    ,
    .(data = list(.SD)),
    by = .(state, kmer)
]

record_linkage_data <- rais %>%
    left_join(
        filiados_grouped,
        by = c("state", "kmer")
    )



# ---------------------------------------------------------------------------- #
setkey(rais, kmer)

# create kmer block and within them, set keys foreach data table
# perform exact linkage by name
# alternatively use fastLink, but this may not be the best route