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

set.seed(1789)
here <- here::here
path <- '/home/brdata/RAIS'
rais_sql <- here('data/output/rais.sqlite3')

source(here('scripts/funs.R'))

# data --------------------------------------------------------------------
files <- list.files(
  path = path,
  pattern = 'RAIS200[3-9]|RAIS201[0-5]',
  full.names = T
)

cpi <- fread(
    here("data/cpi.csv"),
    colClasses = c('year' = 'numeric')
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
  
  year <- str_extract(file, '\\d{4}')
  ref_date <- paste0(year, '-12-31')
  
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
    name = 'rais',
    row.names = F,
    append = T
  )
  
  rm(rais)
  gc()
  
  write_log(file)
}

# audits
load(
  here('data/audits.RData')
)

audits %<>%
  rename(
    cod_ibge_6 = idMun
  )

dbWriteTable(
  con,
  'audits',
  audits,
  row.names = F
)

# extract high bureaucrats ------------------------------------------------
con <- DBI::dbConnect(
  RSQLite::SQLite(),
  here('data', 'rais.sqlite3')
)

rais_bureau <- rais %>% 
  filter(
    nat_jur == 1031 & occupation == 1
  ) %>% 
  filter(
    sql('id_employee IS NOT NULL')
  ) %>% 
  transmute(
    id_employee,
    idMun,
    year,
    action = case_when(
      fired == 1 ~ 1,
      departure == 1 ~ 2,
      T ~ 0
    )
  ) %>% 
  collect()

rais_bureau %>% 
  data.table::fwrite(
    here('data/output/rais_bureau.csv.gz'),
    compress = 'gzip'
  )