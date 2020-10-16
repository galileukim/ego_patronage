# ---------------------------------------------------------------------------- #
# set-up
library(data.table)
library(tidyverse)
library(here)
# ---------------------------------------------------------------------------- #
# read-in filiados
filiado <- fread(here("data/raw/filiado.csv.gz")) %>%
    transmute(
        cod_ibge_6,
        electoral_title = as.double(electoral_title),
        party,
        starts_with("date")
    )

level <- c("mun", "state")

rais_hash <- map(
    sprintf(here("data/clean/id/rais_id_to_filiado_hash_%s.csv"), level),
    fread
)

# join tables on electoral title
setkey(filiado, electoral_title)

filiado_hash <- rais_hash %>%
    map(
        ~ setkey(., electoral_title) %>%
            .[filiado, nomatch = 0]
    )

# trim out identifiers
filiado_hash <- filiado_hash %>% 
    modify(
        ~ select(., -electoral_title)
    )

filenames <- sprintf("~/SHARED/gkim/filiado_with_id_employee_%s.csv", level)

walk2(
    filiado_hash,
    filenames,
    ~ fwrite(.x, .y)
)