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

debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e3, Inf)

# ---------------------------------------------------------------------------- #
id_path <- "/home/BRDATA/RAIS/rawtxt"
years <- 2003:2016

for (i in seq_along(years)) {
    rais_id_hash <- hash()
    n_unique_ids <- rep(NA, length(years))
    t <- years[i]

# ---------------------------------------------------------------------------- #
    print(
        sprintf("start producing hash for year %s", t)
    )

    id_file_path <- list.files(
        sprintf("%s/%s/", id_path, t),
        full.names = T
    )

    id_file_path <- ifelse(isTRUE(debug), id_file_path[1], id_file_path)

    select_cols <- c(
        "CPF",
        ifelse(t <= 2010, "NOME", "Nome Trabalhador")
    )

    select_cols_lower <- str_to_lower(select_cols)

# ---------------------------------------------------------------------------- #
    print("import 7z files")
    rais_id <- map_dfr(
        id_file_path,
        ~ read_7z(., t, select = select_cols)
    )

# ---------------------------------------------------------------------------- #
    print("deduplicate names")
    rais_id_unique <- rais_id %>%
        rename_with( # note that cpf comes first: check select_cols
            ~ c("cpf", "name")
        ) %>%
        extract_unique_id(
            c("cpf", "name")
        ) %>%
        transmute(
            cpf,
            name = clean_name(name)
        )

    n_unique_ids[i] <- nrow(rais_id_unique)

# ---------------------------------------------------------------------------- #
    print("extract id hash")
    rais_id_new_hash <- extract_new_hash(
        rais_id_unique,
        rais_id_hash
    )

    print("update id hash t0")
    rais_id_hash[rais_id_new_hash[["keys"]]] <- rais_id_new_hash[["values"]]

# ---------------------------------------------------------------------------- #
    print("write out hash table")

    length(rais_id_hash)
    print(n_unique_ids)
    
    tibble(
        cpf = keys(rais_id_hash),
        name = values(rais_id_hash, USE.NAMES = F)
    ) %>%
        as.data.table() %>%
        fwrite(
            sprintf(here("data/clean/id/rais_id_hash_%s.csv"), t)
        )

    gc()
}