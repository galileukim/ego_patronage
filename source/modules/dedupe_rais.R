# ---------------------------------------------------------------------------- #
# input: employee identifiers from rais (note that these can be duplicated)
# note: each file partitions the data by year-region
# use hash package in CRAN
# use the key exists for the table
# create a dictionary add
# create a hash
# triage for the merge
# kmer subset: 4 or 5 letters.
# do it on last name only
# output: deduplicated employee identifiers
source(
    here::here("source/modules/setup_preprocess.R")
)

library(haven)
library(hash)
library(digest)
library(microbenchmark)

source(here("source/utils/preprocess_rais.R"))

debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e3, Inf)

# ---------------------------------------------------------------------------- #
id_path <- "/home/BRDATA/RAIS/rawtxt"
years <- 2008:2016
n_unique_ids <- rep(NA, length(years))

for (i in seq_along(years)) {
    print("set up envir vars")
    t <- years[i]

    name_col <- if (t <= 2010) "NOME" else "Nome Trabalhador"
    mun_col <- if(t <= 2010) "MUNICIPIO" else "Município"
    select_cols <- c(mun_col, "CPF", name_col)

    print("initiate extraction for year t")
    id_file_path <- extract_id_file_names(t, debug)
    
    print("import 7z files")
    rais_id_t <- map_dfr(
        id_file_path,
        ~ read_7z(., select = select_cols, t) %>%
            rename_with(
                ~ c("cod_ibge_6", "cpf", "name")
            )
    )

    setkey(rais_id_t, cpf)

    rais_id_t <- rais_id_t %>%
            extract_unique_cpf() %>%
            mutate(
                name = clean_name(name),
                year = t
            )

    print("write out number of entries")
    n_unique_ids[i] <- nrow(rais_id_t)


    print("write out file")
    fwrite(
        rais_id_t,
        sprintf(here("data/clean/id/rais_hash/rais_id_%s.csv.gz"), t),
        compress = "gzip"
    )

    rm(rais_id_t)
    gc()
}

print("create rais hash table: complete!")