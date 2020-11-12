# ---------------------------------------------------------------------------- #
# set-up
library(data.table)
library(tidyverse)
library(here)
# ---------------------------------------------------------------------------- #
# read-in candidates
candidate <- fread(
    here("data/raw/candidate.csv.gz"),
    colClasses = "character"
) %>%
    select(
        cod_ibge_6,
        cpf = cpf_candidate,
        election_year,
        campaign,
        candidate_num,
        candidate_status,
        coalition,
        coalition_name,
        coalition_type,
        cod_tse,
        age,
        edu,
        edu_desc,
        elected,
        election_type,
        gender,
        incumbent,
        occupation,
        occupation_code,
        outcome,
        party,
        position,
        position_code
    )

rais_hash <- fread(
    here("data/clean/id/candidate_id.csv.gz"),
    colClasses = "character"
)

# join tables on electoral title
setkey(candidate, cpf)
setkey(rais_hash, cpf)

candidate_hash <- candidate %>%
    merge(
        rais_hash,
        on = "cpf",
        all = FALSE
    )

# trim out identifiers
candidate_hash <- candidate_hash %>% 
    select(
        -name,
        -electoral_title
    )

filenames <- fwrite(
    candidate_hash,
    "~/SHARED/gkim/candidate_with_id_employee.csv",
    compress = "gzip"
    )