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
sample_size <- ifelse(isTRUE(debug), 1e3, Inf)
# vdigest <- Vectorize(digest::digest)

# ---------------------------------------------------------------------------- #
id_path <- "data/raw/id"
years <- 2003:2004

rais_id_hash <- hash()
for (i in seq_along(years)) {
    t <- years[i]

    print("import 7z files")
    rais_id < read_7z(
        id_path, 
        t, 
        debug = FALSE
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

    print("extract id hash t0")
    rais_id_hash <- hash(
        keys = rais_id %>% pluck(t0, "cpf"),
        values = rais_id %>% pluck(t0, "name")
    )

    print("extract id hash t1")
    rais_id_new_hash <- extract_new_hash(
        rais_id %>% pluck(t1),
        rais_id_hash
    )

    print("update id hash t0")
    rais_id_hash[rais_id_new_hash[["keys"]]] <- rais_id_new_hash[["values"]]
}

rais_id_hash
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