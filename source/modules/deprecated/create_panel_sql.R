# set-up ------------------------------------------------------------------
pacman::p_load(
  tidyverse,
  lubridate,
  data.table,
  haven,
  R.utils,
  dbplyr,
  DBI,
  magrittr
)

here <- here::here
path_to_rais <- "/home/BRDATA/RAIS"
rais_sql <- here("data/clean/rais_panel.sqlite3")

source(here("source/utils/panel.R"))

# data --------------------------------------------------------------------
files <- list.files(
  path_to_rais = path,
  pattern = "RAIS200[3-9]|RAIS201[0-6]",
  full.names = T
)

cpi <- fread(
    here("data/raw/cpi.csv"),
    colClasses = c("year" = "numeric")
  )

# data --------------------------------------------------------------------
# rais panel
if(file.exists(rais_sql)){
  file.remove(rais_sql)
}

src_sqlite(rais_sql, create = T)
con <- DBI::dbConnect(RSQLite::SQLite(), rais_sql)

for(file in files){
  print_log(file)
  
  year <- str_extract(file, "\\d{4}")
  ref_date <- paste0(year, "-12-31")
  
  cpi_year <- cpi %>% 
    filter(year == !!year) %>% 
    pull(cpi)
  
  rais <- read_dta(file)
  
  # fix wage (dollars of 2010) and age
  rais %<>% 
    mutate(
      cpi = !!cpi_year
    )
  
  if(as.numeric(year) <= 2010){
    rais %<>%
      mutate(
        age = calc_age(dtnascimento, !!ref_date),
        edu = grauinstrucao
      ) %>% 
      trim_rais
  }else{
    rais %<>%
      rename(
        age = idade,
        edu = escolaridade
      ) %>% 
      trim_rais
  }
  
  rais %<>%
    year_to_char %>% 
    fix_wage %>% 
    fix_contract
  
  dbWriteTable(
    value = rais,
    conn = con,
    name = "rais",
    row.names = F,
    append = T
  )
  
  rm(rais)
  gc()
  
  write_log(file)
}