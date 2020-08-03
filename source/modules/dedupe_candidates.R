# ---------------------------------------------------------------------------- #
source(
    here::here("source/modules/setup_preprocess.R")
)

debug <- FALSE
sample_size <- if_else(isTRUE(debug), 1e4, Inf)

candidate <- fread(
    here("data/raw/candidate.csv.gz"),
    integer64 = "character",
    nrows = sample_size
)

# ---------------------------------------------------------------------------- #
print("deduplicate candidate data")
# distribution of duplicated entrlsies
# year tendencies
candidate_deduped <- candidate %>%
    filter(
        elec_title != "" & cpf_candidate != ""
    ) %>% 
    distinct(
        elec_title,
        cpf_candidate,
        candidate_name
    ) %>%
    rename(
        name = candidate_name,
        cpf = cpf_candidate
    )

# defective entries occur due to minor misspellings
# decision: retain first observation
defective_entries <- candidate_deduped %>%
    add_count(
        cpf, elec_title
    ) %>%
    filter(
        n > 1
    )

candidate_deduped <- candidate_deduped %>%
    distinct(
        cpf, elec_title,
        .keep_all = T
    ) %>%
    clean_names(
        name
    ) %>%
    arrange_by_name(
        name
    )

# ---------------------------------------------------------------------------- #
print("write-out file")
candidate_deduped %>% 
    fwrite(
        here("data/clean/candidate_deduped.csv.gz"),
        compress = "gzip"
    )