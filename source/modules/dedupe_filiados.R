# ---------------------------------------------------------------------------- #
library(data.table)
library(tidyverse)
library(here)

debug <- FALSE
sample_size <- if_else(isTRUE(debug), 1e4, Inf)

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
# distribution of duplicated entrlsies
# year tendencies
filiados <- filiados %>%
    clean_filiados()

# note: there are defective year entries < 0.5%
filiados <- filiados %>%
   filter(
       between(year_start, 1950, 2019)
   )

filiados_deduped <- filiados %>%
    distinct(
        elec_title,
        name
    ) %>% 
    clean_names(name) %>%
    arrange_by_name(name)

# ---------------------------------------------------------------------------- #
print("write-out file")
filiados_deduped %>% 
    fwrite(
        here("data/clean/filiados_deduped.csv.gz"),
        compress = "gzip"
    )