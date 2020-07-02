# saeb hierarchical
saeb_hlm <- fread(
  here("data/saeb/saeb_hierarchical.csv.gz")
)

# join school covariates
censo_school %<>% 
  filter(
    dep == "municipal", year >= 2001
  ) %>%
  select(
    state,
    cod_ibge_6,
    year,
    location,
    school_id,
    school_name,
    internet,
    kitchen,
    library,
    lab_info,
    meal,
    starts_with('principal'),
    sewer_grid,
    toilet,
    water_grid
  )

join_cols <- c("state", "cod_ibge_6", "school_id", "year")

saeb_hlm %<>%
  mutate(state = str_sub(cod_ibge_6, 1, 2)) %>% 
  rename(school_id = cod_school) %>% 
  mutate_at(join_cols, as.character) %>% 
  left_join(
    censo_school %>%
      mutate_at(join_cols, as.character) %>% 
      select(-location),
    by = join_cols
  )

# turnover
censo_school_turnover <- fread(
  here("data/censo_escolar/censo_school_turnover.csv.gz")
) %>% 
  rename(
    grade = grade_level,
    n_teacher = n,
    n_teacher_lag = n_lag
  )

join_cols <- c(join_cols, "grade")

saeb_hlm %<>%
  mutate_at(join_cols, as.character) %>%
  tidylog::left_join(
    censo_school_turnover %>% mutate_at(join_cols, as.character),
    by = join_cols
  )

# create log pop
saeb_hlm %<>%
  mutate(
    censo_log_pop = log(censo_pop)
  )

# break down teacher wages into quantiles
saeb_hlm %<>%
  mutate(
    saeb_teacher_work_school = if_else(year_working_school_teacher == "", NA_character_, year_working_school_teacher),
    grade = recode(grade, `4` = "5", `8` = "9")
  )

saeb_hlm %>% 
  fwrite_gz(
    here("data/saeb/saeb_hierarchical.csv")
  )
