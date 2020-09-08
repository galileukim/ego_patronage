# input: rais_id_employee
# goal: verify whether there are duplicates in the data (take sample)
# output: diagnostics verifying prevalence of duplicated names
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
