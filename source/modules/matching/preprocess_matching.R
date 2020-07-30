# input: filiados (partisan affiliation) and rais (employment data)
# note that there are some duplicated names, but they are rare
# also note that the majority of party members have only one affiliation
# the majority of duplicates in RAIS is due to holding several jobs
# final output: unique id and partisanship per year per state
library(tidyverse)
library(data.table)
library(haven)
library(here)

source(here("source/utils/preprocess.R"))

year <- 2006:2016
path_out <- "data/clean/preprocessed"

fwrite <- partial(data.table::fwrite, compress = "gzip")

print("clean-up filiados and write-out")
filiados <- fread(
    "data/raw/filiado_cpf.csv",
    nrows = 1e4,
    integer64 = "character"
)

filiados %>%
    clean_filiados() %>%
    fwrite(
        here(path_out, "filiados_clean.csv.gz")
    )

print("clean-up rais and write-out")
path_rais_out <- here(path_out, sprintf("rais_clean_%s.csv.gz", year))

walk2(
    year,
    path_rais_out,
    ~ read_rais(
        year = .x
    ) %>%
        clean_rais() %>%
        fwrite(
            file = .y
        )
)

# most efficient solution is actually to create an rsqlite database