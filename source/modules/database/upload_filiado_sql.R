# ---------------------------------------------------------------------------- #
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
filiado_mun <- fread(
    here_data("id/filiado_with_id_employee_mun.csv.gz"),
    integer64 = "character"
)

dbWriteTable(
    rais_con,
    "filiado_mun",
    filiado_mun,
    overwrite = TRUE
)