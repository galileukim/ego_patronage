print("read in data")

rais_id_path <- here("data/clean/id/rais_hash")

rais_id_files <- list.files(
    rais_id_path,
    full.names = T
)

# party membership data
filiados <- fread(
    here("data/clean/id/filiado_id_without_cpf.csv.gz"),
    select = c(
        "state", "name", "electoral_title", "year_start", "year_termination"
    )
)

# note that this data is still incomplete: need to push files from latest data
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