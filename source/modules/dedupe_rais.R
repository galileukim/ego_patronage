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

debug <- T
sample_size <- ifelse(isTRUE(debug), 1e3, Inf)
vdigest <- Vectorize(digest::digest)

# ---------------------------------------------------------------------------- #
id_path <- "data/raw/id"
year <- 2003

rais_id <- read_7z(
    id_path,
    2003,
    debug = T
)

rais_id <- rais_id %>%
    extract_unique_id(
        c("cpf", "nome")
    ) %>%
    transmute(
        cpf = str_pad(cpf, 11, "left", "0"),
        name = clean_name(nome)
    )

# # create unique keys by combining name and id_employee
# rais_key <- rais_id %>%
#     mutate(
#         key = vdigest(paste(cpf, name), algo = "xxhash32")
#     ) %>%
#     select(key) %>%
#     pull()

# names(rais_key) <- NULL

rais_id_hash <- hash(
    keys = rais_id[["cpf"]],
    values = rais_id[["name"]]
)

rais_id_t1 <- read_7z(
    id_path,
    2004,
    debug = T
)

rais_id_t1 <- rais_id_t1 %>%
    extract_unique_id(
        c("cpf", "nome")
    ) %>%
    transmute(
        cpf = str_pad(cpf, 11, "left", "0"),
        name = clean_name(nome)
    )

rais_id_t1_keys <- rais_id_t1[["cpf"]]
rais_id_t1_values <- rais_id_t1[["name"]]

index <- !has.key(rais_id_t1_keys, rais_id_hash)
names(index) <- NULL

rais_id_t1_keys <- rais_id_t1_keys[index]
rais_id_t1_values <- rais_id_t1_values[index]

rais_id_hash[rais_id_t1_keys] <- rais_id_t1_values
# probably has to loop? wtf is going on

# ---------------------------------------------------------------------------- #

# if(year > 2003){
#     identifiers_previous_year <- fread(
#         sprintf(here("data/clean/id/rais_id_deduped_%s.csv"), year - 1)
#     )

#     identifiers_new <- identifiers_deduped %>%
#         anti_join(
#             identifiers_previous_year,
#             by = c("cpf")
#         )

#     identifiers_deduped <- bind_rows(
#         identifiers_previous_year,
#         identifiers_new
#     )

#     identifiers_deduped <- identifiers[order(name)]
# }

identifiers_deduped %>%
    fwrite(
        sprintf(here("data/clean/rais_id_deduped_%s.csv"), year)
    )

# create anti-join and find new entries
# dedupe and append these