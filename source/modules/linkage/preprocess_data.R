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
        "cod_ibge_6", "name", "electoral_title", "year_start", "year_termination"
    )
)

# note that this data is still incomplete: need to push files from latest data
filiados <- filiados %>%
    setkey(
        year_start, year_termination
    )