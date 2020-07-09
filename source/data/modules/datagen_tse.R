# ==============================================================================
# incorporate data from electoral results
# to-do: finish cleaning the filiados data and prep it for merge with RAIS
# ==============================================================================
chamber <- read_data(
  "raw",
  "tse",
  "election_chamber.csv"
)

chamber <- chamber %>%
  filter(
    election_year %in% seq(2000, 2016, 4)
  ) %>%
  group_by(cod_ibge_6) %>%
  mutate(
    mayor_party_lag = dplyr::lag(mayor_party, order_by = election_year),
    mayor_coalition_lag = dplyr::lag(mayor_coalition, order_by = election_year)
  ) %>%
  ungroup() %>%
  mutate(
    party_turnover = if_else(mayor_party != mayor_party_lag, 1, 0)
  )

chamber %>%
  write_data(
    "tse",
    "election.rds"
  )

quotient <- read_data(
  "raw",
  "tse",
  "election_coalition.csv"
)

quotient %>%
  write_data(
    "tse",
    "electoral_quotient.rds"
  )

election <- read_data(
  "raw",
  "tse",
  "election.csv.gz"
)

mayor <- election %>%
  filter(
    election_year %in% seq(2000, 2016, 4),
    position == "prefeito"
  )

vereador <- election %>%
  filter(
    election_year %in% seq(2000, 2016, 4),
    position != "prefeito"
  ) %>%
  mutate(
    age = election_year - as.numeric(birthyear)
  )

# write-out
mayor %>%
  write_data(
    "tse",
    "mayor.rds"
  )

vereador %>%
  write_data(
    "tse",
    "vereador.rds"
  )`

# ============================================================================
# clean filiados data
# ============================================================================
filiado <- read_data(
  dir = "tse",
  file = "filiado_cpf.csv.gz"
  )

filiado_debug <- filiado %>%
  sample_n(1e3)

# fix year that membership ends.
filiado_debug_with_spells <- filiado_debug %>%
  mutate(
    year_end = if_else(year_end == 2105, 2015L, year_end), # fix defective entries for 2015
    year_membership_end = case_when(
      is.na(year_end) & member_status == "regular" ~ 2019L, # note that the last year available is 2019
      T ~ year_end
    ),
    year_membership_end = pmax(year_membership_end, year_cancel, na.rm = T),
    membership_spell_years = map2(
      year_start, year_membership_end, 
      ~c(.x, .y)
    )
  ) %>%
  unnest(membership_spell_years)

filiado_complete_years <- filiado_debug_with_spells %>% 
  rename(
    year = membership_spell_years
  ) %>%
  complete_year_by_group(
    .group_vars = c("member_name", "elec_title"),
    complete_years = 2006:2015
  )

filiado_complete <- filiado_debug_with_spells %>%
  left_join(
    filiado_complete_years
  )

# to-do: figure out how to store this data efficeintly: it will be huge