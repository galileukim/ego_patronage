
# patronage ---------------------------------------------------------------
election <- fread(
  here("data/tse/election.csv")
)

mayor <- fread(
  here("data/tse/mayor.csv")
) %>% 
  mutate_all(na_if, "") %>% 
  filter(!is.na(cpf_candidate))

mayor %<>%
  group_by(
    cod_ibge_6,
    election_year
  ) %>% 
  filter(
    !any(round == 2)
  ) %>% 
  ungroup()

vereador <- fread(
  here('data/tse/vereador.csv.gz')
) 

vereador %<>%
  mutate_all(na_if, "") %>% 
  filter(!is.na(cpf_candidate))

rais_mun <- fread(
  here("data/rais/rais_mun.csv.gz")
)

censo <- fread(
  here("data/censo_br/censo_2000.csv")
)

patronage_mun <- rais_mun %>% 
  filter(
    !is.na(cbo_category) & cbo_category != ""
    # cbo_category == 'education'
  ) %>% 
  filter(
    year >= 2003
  ) %>% 
  group_by(
    cbo_category,
    cod_ibge_6,
    election_year
  ) %>% 
  summarise(
    prop_hired = sum(mean_hired*total)/sum(total),
    prop_fired = sum(mean_fired*total)/sum(total),
    total = sum(total)
  ) %>% 
  ungroup

patronage_mayor <- mayor %>% 
  filter(
    incumbent == 1
  ) %>% 
  ungroup() %>% 
  inner_join(
    patronage_mun %>% 
      mutate(election_year = election_year + 4),
    by = c('cod_ibge_6', 'election_year')
  ) %>% 
  left_join(
    censo,
    by = c("cod_ibge_6")
  ) %>% 
  mutate_at(
    vars(state, election_year),
    as.factor
  ) 

patronage_councilor <- vereador %>%
  left_join(
    vereador %>% 
      filter(elected == 1) %>% 
      transmute(
        cod_ibge_6,
        election_year_lag = election_year,
        election_year = election_year + 4,
        cpf_candidate,
        party_lag = party,
        elected_lag = elected
      ),
    by = c("cpf_candidate", "cod_ibge_6", "election_year")
  ) %>% 
  filter(
    incumbent == 1
  )

patronage_councilor <- patronage_councilor %>% 
  left_join(
    mayor %>% 
      filter(elected == 1) %>% 
      transmute(cod_ibge_6, election_year = election_year + 4, mayor_coalition = coalition),
    by = c('cod_ibge_6', 'election_year')
  ) %>% 
  mutate(
    mayor_coalition_member = if_else(
      str_detect(mayor_coalition, paste0("\\b", party_lag, "\\b")),
      'coalition_member', 'non_coalition_member'
    ) %>% 
      fct_relevel('non_coalition_member')
  ) %>% 
  filter(
    !is.na(mayor_coalition_member)
  )

patronage_councilor %<>%
  inner_join(
    patronage_mun %>% 
      mutate(
        election_year = election_year + 4
      ),
    by = c('cod_ibge_6', 'election_year')
  )

patronage_councilor %<>% 
  left_join(
    censo,
    by = c("cod_ibge_6")
  )

patronage_councilor %<>%
  mutate_at(
    vars(state, election_year),
    as.factor
  ) 

patronage_reelection <- bind_rows(
  patronage_mayor,
  patronage_councilor
)

patronage_reelection %<>%
  mutate(
    type = if_else(
      position == 'prefeito',
      'mayor', 
      as.character(mayor_coalition_member)
    ) %>% 
      fct_relabel(~str_replace_all(., "_", " ")) %>% 
      fct_relevel(
        "mayor"
      ),
    party_fct = case_when(
      !(party %in% c('pt', 'psdb', 'pmdb')) ~ 'other',
      T ~ party
    )
  )

patronage_reelection %>% 
  fwrite(
    here('data/patronage/patronage_reelection.csv.gz'),
    compress = 'gzip'
  )
