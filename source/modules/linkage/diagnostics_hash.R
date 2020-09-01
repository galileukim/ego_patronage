# input: a) rais data
# output: table of diagnostics for rais that identifies duplicate names
source(
    here::here("source/modules/setup_preprocess.R")
)

source(
    here("source/utils/diagnostics_linkage.R")
)

sample_size <- Inf

# ---------------------------------------------------------------------------- #
rais_hash_file <- here("data/clean/id/rais_filiado_crosswalk.csv") 

rais_hash_diagnostics <- rais_hash[
    ,
    .(
        count_title = uniqueN(electoral_title),
        count_cpf = uniqueN(cpf)
    ),
    by = .(state, name, year)
]

message("filter out names with multiple titles or cpf")
names_with_multiple_cpfs <- rais_hash_diagnostics[
    count_cpf + count_title > 2, .(name)
]

number_of_defective_entries <- nrow(names_with_multiple_cpfs)

rais_hash_duplicated_names <- rais_hash[names_with_multiple_cpfs, on = "name"]
rais_hash_duplicated_names[
    ,
    .(count_year = .N),
    by = year
]

message(
    "there are ", number_of_defective_entries, 
    " duplicated names in the hash table."
)