# set-up ------------------------------------------------------------------
library(tidyverse)
library(data.table)
library(haven)
library(R.utils)
library(dbplyr)
library(DBI)

set.seed(1789)
here <- here::here
path <- "/home/brdata/RAIS"
debug <- TRUE
nrows <- if (isTRUE(debug)) 1e3 else Inf
rais_sql <- here("data/database/rais.sqlite3")

source(
  here("source/modules/database/requirements.R")
)

# data --------------------------------------------------------------------
files <- list.files(
  path = path,
  pattern = "RAIS200[7-9]|RAIS201[0-6]",
  full.names = T
)

cpi <- fread(
  here("data/raw/cpi.csv"),
  colClasses = c("year" = "numeric")
)

# data --------------------------------------------------------------------
# rais panel
if (file.exists(rais_sql)) {
  file.remove(rais_sql)
}

src_sqlite(rais_sql, create = T)
con <- DBI::dbConnect(RSQLite::SQLite(), rais_sql)

for (file in files) {
  print_log(file)

  year <- str_extract(file, "\\d{4}")
  ref_date <- paste0(year, "-12-31")

  cpi_year <- cpi %>%
    filter(year == !!year) %>%
    pull(cpi)

  rais <- read_dta(file, n_max = 100)

  # fix wage (dollars of 2010) and age
  rais <- rais %>%
    mutate(
      cpi = !!cpi_year
    )

  if (as.numeric(year) <= 2010) {
    rais <- rais %>%
      mutate(
        age = calc_age(dtnascimento, !!ref_date),
        edu = grauinstrucao
      ) %>%
      trim_rais()
  } else {
    rais <- rais %>%
      rename(
        age = idade,
        edu = escolaridade
      ) %>%
      trim_rais()
  }

  rais <- rais %>%
    year_to_char() %>%
    fix_wage() %>%
    fix_contract()

  dbWriteTable(
    value = rais,
    conn = con,
    name = "rais",
    row.names = F,
    append = TRUE
  )

  rm(rais)
  gc()

  write_log(file)
}
