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
# vdigest <- Vectorize(digest::digest)

# ---------------------------------------------------------------------------- #
id_path <- "data/raw/id"
years <- 2003:2004

rais_id_hash <- hash()
n_unique_ids <- rep(NA, length(years))

for (i in seq_along(years)) {
    t <- years[i]

    print("import 7z files")
    rais_id <- read_7z(
        id_path, 
        t, 
        debug = debug
    )

    print("deduplicate names")
    rais_id_unique <- rais_id %>%
        extract_unique_id(
            c("cpf", "nome")
        ) %>%
        transmute(
            cpf = str_pad(cpf, 11, "left", "0"),
            name = clean_name(nome)
        )

    n_unique_ids[i] <- nrow(rais_id_unique)

    print("check if there are multiple names per cpf...")
    n_names_per_cpf <- rais_id_unique %>%
        group_by(cpf) %>%
        mutate(
            n = n_distinct(name)
        ) %>%
        pull(n)
    
    multiple_names_per_cpf <- any(n_names_per_cpf > 1)

    if(isTRUE(multiple_names_per_cpf)){
        print(
            sprintf("there are multiple name entries per cpf for year %s,", years[i])
        )
    }else{
        print(
            sprintf("all name entries per cpf for year %s are unique!", years[i])
        )
    }

    print("extract id hash")
    rais_id_new_hash <- extract_new_hash(
        rais_id_unique,
        rais_id_hash
    )

    print("update id hash t0")
    rais_id_hash[rais_id_new_hash[["keys"]]] <- rais_id_new_hash[["values"]]

    gc()
}

length(rais_id_hash)
print(n_unique_ids)

rais_id_hash %>%
    as.list() %>%
    as.data.frame() %>%
    fwrite(
        sprintf(here("data/clean/rais_id_hash.csv"), year),
        compress = "gzip"
    )