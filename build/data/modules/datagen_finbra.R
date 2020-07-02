# finbra ------------------------------------------------------------------
# budgetary expenditures for each municipality
# unit of analysis: municipality-year

finbra <- read_data("raw", "finbra", "despesa_mun.csv") %>%
  filter(
    year >= 2000
  ) %>%
  select(
    cod_ibge_6,
    year,
    total = total_expenditure,
    legislative,
    administration,
    health = health_sanitation,
    education = education_culture,
    industry = industry_commerce
  )

finbra <- finbra %>%
  setNames(
    c(
      "cod_ibge_6", "year",
      paste0("budget_", names(.)[3:length(names(.))])
    )
  )

# write-out
finbra %>% 
  write_data(
    "finbra", "finbra.rds"
  )