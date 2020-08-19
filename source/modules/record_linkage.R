# input: a) filiado data
# b) rais data
# output: table containing the linkage between electoral_title and cpf
# notes: create a unique identifier that tracks row number and file id
# modularize each function to perform the linkage within each block
# nest, nest, nest!
source(
    here::here("source/modules/setup_preprocess.R")
)

debug <- TRUE
sample_size <- ifelse(isTRUE(debug), 1e5, Inf)

rais_id_path <- here("data/clean/id/rais_hash")

rais_id_files <- list.files(
    rais_id_path,
    full.names = T
)

# ---------------------------------------------------------------------------- #
rais <- fread(
    rais_id_files[1]
)

filiados <- fread(
    here("data/clean/id/filiado_id_without_cpf.csv.gz")
)

# ---------------------------------------------------------------------------- #
rais <- rais %>%
    mutate(
        kmer = substr(name, 1, 3)
    )

filiados <- filiados %>%
    mutate(
        kmer = substr(name, 1, 3)
    )

# ---------------------------------------------------------------------------- #
rais 

# create kmer block and within them, set keys foreach data table
# perform exact linkage by name
# alternatively use fastLink, but this may not be the best route