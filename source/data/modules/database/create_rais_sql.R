# set-up ------------------------------------------------------------------c
library(tidyverse)
library(data.table)
library(haven)
library(R.utils)
library(dbplyr)
library(DBI)

set.seed(1789)

here <- here::here
path <- "/home/brdata/RAIS"

rais_files <- list.files(
  path = path,
  pattern = "RAIS200[3-9]|RAIS201[0-6]",
  full.names = T
)

log_file <- here("log/log_create_sql.txt")
debug <- FALSE

source(
  here("source/data/modules/database/requirements.R")
)

# data --------------------------------------------------------------------
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
rais_con <- DBI::dbConnect(RSQLite::SQLite(), rais_sql)

for (file in rais_files) {
  print_log(file, filepath = log_file)

  year <- str_extract(file, "\\d{4}")
  ref_date <- paste0(year, "-12-31")

  cpi_year <- cpi %>%
    filter(year == !!year) %>%
    pull(cpi)

  rais <- read_dta(file, n_max = nrows)

  # fix wage (dollars of 2010) and age
  rais <- rais %>%
    mutate(
      cpi = !!cpi_year
    )

  # fix cnae var and include it in final table
  rais <- rais %>%
    rename_with(
      ~ str_replace(., "cnae([0-9]{2})classe", "cnae_\\1"),
      matches("cnae[0-9]{2}classe")
    )

  # if first year, generate placeholder for cnae_20
  if(as.numeric(year == 2003)){
    rais <- rais %>%
      mutate(
        cnae_20 = NA_real_
      )
  }

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
    fix_contract() %>%
    exclude_id_null()

  dbWriteTable(
    value = rais,
    conn = rais_con,
    name = "rais",
    row.names = F,
    append = TRUE
  )

  rm(rais)
  gc()

  write_log(file, filepath = log_file)
}

# ==============================================================================
# generate indexes to spee dd up query
# ==============================================================================
dbExecute(
  "
  CREATE INDEX IF NOT EXISTS natureza_juridica_idx ON
  rais(nat_jur)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS year_idx ON
  rais(year)
  "
)
# ==============================================================================
# generate sample sql file
# ==============================================================================
rais_sample_con <- DBI::dbConnect(
  RSQLite::SQLite(),
  here("data/clean/database/rais_sample.sqlite3")
  )

rais_sample <- dbGetQuery(
  "
  SELECT * FROM rais
  WHERE cod_ibge_6 = 110001
  "
)

rais_sample %>%
  dbWriteTable(
    rais_sample_con,
    name = "rais",
    value = .,
    overwrite = TRUE
  )
