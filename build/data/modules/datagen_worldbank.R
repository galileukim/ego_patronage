edu_global <- read_data(
  "raw",
  "worldbank",
  "global_edu_indicators.csv"
) %>% 
  filter(
    between(year, 2000, 2016)
  )

edu_global %<>%
  mutate(
    pisa = rowMeans(select(., starts_with("pisa_")), na.rm = T)
  ) %>% 
  select(
    -starts_with("pisa_")
  )

pisa_global <- edu_global %>%
  group_by(year) %>% 
  summarise_at(
    vars(pisa),
    mean, na.rm = T
  ) %>% 
  mutate(
    type = "global"
  )

edu_global %<>% 
  mutate(
    type = case_when(
      # country_code == "lcn" ~ "la",
      country_code == "bra" ~ "br",
      country_name == "world" ~ "global",
      T ~ NA_character_
    )
  ) %>% 
  filter(
    !is.na(type)
  )

edu_global %<>% 
  group_by(
    type,
    year
  ) %>% 
  select_if(is.numeric) %>% 
  summarise_all(
    mean,
    na.rm = T
  ) %>% 
  ungroup()

edu_global %<>% 
  left_join(
    pisa_global,
    by = c("type", "year")
  ) %>% 
  mutate(
    pisa = coalesce(pisa.x, pisa.y)
  ) %>% 
  select(-starts_with("pisa."))

edu_global %>%
    write_data(
        "worldbank",
        "edu_global.rds"
    )