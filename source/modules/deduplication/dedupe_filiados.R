# ---------------------------------------------------------------------------- #
# do a trial run with n_row = 1e5
# large enough that the run time tells you something meaningful
# monitor memory usage
# consider using a deterministic hash function (removing duplicates there)
# hash package (in CRAN)
# ignore new entries
# by its numeral
library(data.table)
library(tidyverse)
library(here)

debug <- FALSE
sample_size <- if_else(isTRUE(debug), 1e3, Inf)

source(
    here("source/utils/preprocess.R")
)

filiados <- fread(
    here("data/raw/filiado.csv.gz"),
    integer64 = "character",
    nrows = sample_size
)

# ---------------------------------------------------------------------------- #
print("deduplicate filiados data")
# distribution of duplicated entries
# year tendencies
filiados <- filiados %>%
    unique(
        by = c("member_name", "electoral_title", "party")
    ) %>%
    mutate(
        name = clean_name(member_name)
    )

# # note: there are defective year entries < 0.5%
# filiados <- filiados %>%
#    filter(
#        between(year_start, 1950, 2019)
#    )

filiados_ordered <- filiados[order(name)]

# ---------------------------------------------------------------------------- #
print("write-out file")
filiados_ordered %>%
    fwrite(
        here("data/clean/id/filiado_deduped.csv.gz"),
        compress = "gzip"
    )