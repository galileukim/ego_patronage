# set-up ------------------------------------------------------------------
lapply(
  c("tidyverse", "data.table", "here"),
  require,
  character.only = T
)

set.seed(1789)

source(here("scripts/thesis_setup.R"))
# eda ---------------------------------------------------------------------
mods <- list()

mods$patronage <- lm(
  log(budget_total/censo_pop + 0.1) ~ rais_hired*coalition_share + as.factor(year) + as.factor(state) +
    censo_median_wage + chamber_size,
  data = cabinet_mun
)

mods$coalition <- lm(
  log(rais_hired + 0.1) ~ coalition_share + share_vote_mayor + censo_rural + censo_log_pop +
    as.factor(year) + as.factor(state) + as.factor(mayor_party) + as.factor(mayor_edu_desc) +
    effective_parties + mayor_coalition_size + party_turnover + 
    mayor_reelected,
  data = cabinet_mun %>% 
    mutate(
      share_vote_mayor = total_vote_mayor/total_vote_legislative
    )
)

map(
  mods,
  summary
)

# ideology ----------------------------------------------------------------
ideology <- fread(
  "~/Dropbox/Value_new_Candidates/data/ideology/candidate_ideology_local.csv"
) %>% 
  select(
    cpf_candidate,
    cfscore
  )

# join election with ideology
election <- election %>% 
  left_join(
    ideology,
    by = "cpf_candidate"
  )

# join
rais_mun <- rais_mun %>% 
  left_join(
    election,
    by = c("cod_ibge_6", "election_year")
  )

lm(
  rais_hired ~ as.factor(rais_category) + coalition_share + cfscore +
    as.factor(year) + as.factor(str_sub(cod_ibge_6, 1, 2)) ,
  data = rais_mun
) %>% 
  summary
