# ---------------------------------------------------------------------------- #
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
    clean_filiados() %>%
    clean_names(name)

# note: there are defective year entries < 0.5%
filiados <- filiados %>%
   filter(
       between(year_start, 1950, 2019)
   )

filiados_ordered <- filiados[order(last_name)]

# ---------------------------------------------------------------------------- #
print("write-out file")
filiados_ordered %>% 
    fwrite(
        here("data/clean/filiados_deduped.csv.gz"),
        compress = "gzip"
    )