
# ==============================================================================
# generate indexes to speed up query
# ==============================================================================
source(
    here::here("source/data/modules/database/globals.R")
)

source(
    here("source/data/modules/database/requirements.R")
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS natureza_juridica_idx ON
  rais(nat_jur)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS id_employee_idx ON
  rais(id_employee)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS mun_idx ON
  rais(cod_ibge_6)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS year_idx ON
  rais(year)
  "
)

# rais entry

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS entry_id_employee_idx ON
  rais_bureaucrat_entry(id_employee)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS entry_year_idx ON
  rais_bureaucrat_entry(year)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS entry_mun_idx ON
  rais_bureaucrat_entry(cod_ibge_6)
  "
)

# filiado_mun
dbExecute(
  "
  CREATE INDEX IF NOT EXISTS filiado_id_employee_idx ON
  filiado_mun(id_employee)
  "
)

dbExecute(
  "
  CREATE INDEX IF NOT EXISTS filiado_mun_idx ON
  filiado_mun(cod_ibge_6)
  "
)
