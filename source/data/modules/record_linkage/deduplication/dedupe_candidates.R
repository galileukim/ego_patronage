# ---------------------------------------------------------------------------- #
debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e4, Inf)

source(
    here::here("source/modules/setup_preprocess.R")
)

candidate <- fread(
    here("data/raw/candidate.csv.gz")
)

# ---------------------------------------------------------------------------- #
print("deduplicate candidate data")
# distribution of duplicated entrlsies
# year tendencies
candidate_deduped <- candidate %>%
    filter(
        elec_title != "" & cpf_candidate != ""
    ) %>% 
    unique(
        by = c("elec_title", "cpf_candidate")
    ) %>%
    transmute(
        electoral_title = elec_title,
        cpf = cpf_candidate,
        name = clean_name(candidate_name)
    )

# ---------------------------------------------------------------------------- #
print("write-out file")
candidate_deduped %>% 
    fwrite(
        here("data/clean/candidate_deduped.csv.gz"),
        compress = "gzip"
    )