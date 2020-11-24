# ---------------------------------------------------------------------------- #
message('setting up...')

library(tidyverse)
library(dbplyr)
library(data.table)
library(here)
library(DBI)

debug <- TRUE
data_path <- "data/clean/"
source_path <- "source/modules/database/"
rais_con <- DBI::dbConnect(RSQLite::SQLite(), rais_sql)

here_data <- partial(
    here,
    data_path
)

here_source <- partial(
    here,
    source_path
)

source(
    here_source("requirements.R")
)

# ---------------------------------------------------------------------------- #
message("initiating upload")

levels <- c("mun", "state")
files <- sprintf("id/filiado_with_id_employee_%s.csv.gz", levels) %>%
    here_data()

filiado <- files %>%
    map(
        fread,
        integer64 = "character"
    ) %>%
    set_names(levels)

pwalk(
    list(
        name = sprintf("filiado_%s", levels),
        value = filiado
    ),
    dbWriteTable,
    conn = rais_con,
    overwrite = TRUE
)

message("uploading filiados data complete!")