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
# make a more concrete finding
# document those findings
# output: deduplicated employee identifiers
source(
    here::here("source/modules/setup_preprocess.R")
)

library(haven)

debug <- T
sample_size <- ifelse(isTRUE(debug), 1e3, Inf)


id_path <- "data/raw/id"
year <- 2003

# ---------------------------------------------------------------------------- #
extract_7z_file(
    id_path,
    year
)

id_file_path <- list.files(
    tempdir(),
    full.names = T
)

identifiers <- map_dfr(
    id_file_path[1:2],
    read_dta,
    n_max = 1e3
)

file.remove(
    id_file_path
)

identifiers_deduped <- identifiers %>%
    extract_unique_id(
        c("cpf", "nome")
    ) %>%
    transmute(
        cpf,
        name = clean_name(nome)
    )

if(year > 2003){
    identifiers_previous_year <- fread(
        sprintf(here("data/clean/id/rais_id_deduped_%s.csv"), year - 1)
    )

    identifiers_new <- identifiers_deduped %>%
        anti_join(
            identifiers_previous_year,
            by = c("cpf")
        )
    
    identifiers_deduped <- bind_rows(
        identifiers_previous_year,
        identifiers_new
    )

    identifiers_deduped <- identifiers[order(name)]
}

identifiers_deduped %>%
    fwrite(
        sprintf(here("data/clean/rais_id_deduped_%s.csv"), year)
    )

# create anti-join and find new entries
# dedupe and append these