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

debug <- TRUE
sample_size <- ifelse(isTRUE(debug), 1e6, Inf)

# ---------------------------------------------------------------------------- #
id_path <- "/home/BRDATA/RAIS/rawtxt"
years <- 2003
n_unique_ids <- rep(NA, length(years))

rais_id_hash <- hash()

for (i in seq_along(years)) {
    t <- years[i]

    id_file_path <- extract_id_file_names(t)
    
    print("import 7z files")
    rais_id <- map_dfr(
        id_file_path,
        ~ read_7z(., t) %>%
            deduplicate_names
    )

    print("write out number of entries")
    n_unique_ids[i] <- nrow(rais_id)

    print("extract id hash")
    rais_id_hash <- update_hash(
        rais_id_hash,
        rais_id
    )

    print("write out file")
    write_rds(
        rais_id_hash,
        sprintf(
            here("data/clean/id/rais_hash/rais_id_hash_%s.csv", t)
        ),
        compress = "gz"
    )

    rm(rais_id)
    gc()
}

print("create rais hash table: complete!")