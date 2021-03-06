library(tidyverse)
library(dbplyr)
library(data.table)
library(here)
library(DBI)
library(sf)

debug <- ifelse(
    Sys.getenv("USER") == "gali" & dirname(Sys.getenv("PWD")) != "/hdd",
    TRUE,
    FALSE
)

data_path <- "data/clean/"
source_path <- "source/modules/database/"
rais_sql <- here(
  sprintf(
    "data/clean/database/rais%s.sqlite3",
    ifelse(isTRUE(debug), "_sample", "")
  )
)

rais_con <- DBI::dbConnect(RSQLite::SQLite(), rais_sql)