# saeb
import_data(
  here("data/saeb"),
  "exam_mun|hierarchical|exam_school"
) %>%
  assign_data

saeb_dep <- fread(
  here("data/saeb/saeb_exam_dep.csv")
)

# rais
rais_edu <- fread(
  here("data/rais/rais_edu.csv.gz")
) %>% 
  filter(
    year >= 2005
  )

rais_edu_mun <- fread(
  here('data/rais/rais_edu_mun.csv')
) %>% 
  join_covariate() %>% 
  mutate(
    state = str_sub(cod_ibge_6, 1, 2)
  ) %>% 
  mutate_if(
    is.double,
    rescale
  )

rais_mun <- fread(
  here("data/rais/rais_mun.csv.gz")
)

rais_fit <- rais_edu %>% 
  join_covariate() %>%
  mutate(
    state = str_sub(cod_ibge_6, 1, 2),
    coalition_share = rescale(coalition_share),
    rais_category = fct_relevel(rais_category, 'teacher')
  )

# censo escolar
censo_class <- fread(
  here("data/censo_escolar/censo_class.csv.gz")
) %>% 
  group_by(
    year,
    dep,
    grade_level
  ) %>%
  summarise(
    num_enroll = sum(num_enroll)
  ) %>% 
  mutate(
    freq = num_enroll/sum(num_enroll)
  )

censo_school <- fread(
  here('data/censo_escolar/censo_school.csv.gz')
)

censo_school_turnover <- fread(
  here("data/censo_escolar/censo_school_turnover.csv.gz")
)

censo_turnover_ce <- censo_school_turnover %>% 
  filter(
    state == "23",
    grade_level %in% c(2, 5, 9)
  )

censo_mun_turnover <- fread(
  here("data/censo_escolar/censo_mun_turnover.csv.gz")
) %>% 
  group_by(
    cod_ibge_6, year
  ) %>% 
  summarise_stats(
    turnover_index
  ) %>% 
  join_covariate() %>% 
  mutate(
    state = str_sub(cod_ibge_6, 1, 2)
  ) %>% 
  mutate_if(
    is.double,
    rescale
  )

spaece <- fread(
  here("data/spaece/spaece.csv")
)

censo_turnover_score <- censo_turnover_ce %>% 
  left_join(
    censo_school %>% 
      filter(dep == 'municipal', year >= 2007) %>% 
      transmute(school_id = as.integer(school_id), year, toilet, meal),
    by = c('school_id', 'year')
  ) %>% 
  left_join(
    spaece,
    by = c("cod_ibge_6", "school_id", "year", "grade_level" = "grade")
  ) %>% 
  left_join(
    saeb_exam_school,
    by = c("cod_ibge_6", "school_id", "year", "grade_level" = "grade")
  ) %>% 
  join_covariate() %>% 
  mutate(
    mandate_year = if_else(
      year %in% seq(2005, 2013, 4), 1, 0
    )
  )

censo_turnover_score <- censo_turnover_score %>% 
  mutate(
    state = str_sub(cod_ibge_6, 1, 2)
  ) %>% 
  mutate_at(
    vars(grade_level, year, state),
    as.factor
  ) %>% 
  mutate_if(
    is.double,
    rescale
  )

# student test scores
saeb_hierarchical %<>%
  mutate(
    state = str_sub(cod_ibge_6, 1, 2),
    grade = as.factor(grade)
  ) %>% 
  mutate_if(
    is.double,
    rescale
  ) %>% 
  mutate_at(
    vars(saeb_principal_experience, saeb_teacher_work_school, year_as_teacher),
    ~fct_relevel(., "less than 2")
  ) %>% 
  mutate(
    year_as_teacher = if_else(year_as_teacher == "", NA_character_, as.character(year_as_teacher)),
    saeb_wage_teacher = fct_relevel(saeb_wage_teacher, "less than 1000"),
    education_teacher = fct_relevel(education_teacher, "lower school")
  )

saeb_exam_school <- saeb_exam_school %>%
  left_join(
    censo_class,
    by = c("cod_ibge_6", "year", "grade")
  ) %>% 
  mutate(
    weight = attendees/num_enroll,
    weight = if_else(weight > 1, 1, weight),
    mean_grade_exam_wgt = weight*mean_grade_exam
  )


# electoral data
election <- fread(
  here("data/tse/election.csv")
)

mayor <- fread(
  here("data/tse/mayor.csv")
)

# accountability estimation
censo <- fread(
  here("data/censo_br/censo_2000.csv")
)

saeb_election <- saeb_exam_mun %>% 
  group_by(
    cod_ibge_6,
    election_year
  ) %>% 
  summarise(
    mean_grade_exam = mean(mean_grade_exam, na.rm = T)
  ) %>%
  group_by(
    cod_ibge_6
  ) %>% 
  mutate(
    mean_grade_exam_lag_1 = dplyr::lag(mean_grade_exam, 1, order_by = election_year),
    mean_grade_exam_lag_2 = dplyr::lag(mean_grade_exam, 2, order_by = election_year),
    delta_mean_grade_exam = 100*(mean_grade_exam_lag_1 - mean_grade_exam_lag_2)/mean_grade_exam_lag_2
  ) %>% 
  ungroup()

accountability <- mayor %>% 
  group_by(
    cpf_candidate
  ) %>% 
  arrange(
    desc(election_year)
  ) %>% 
  mutate(
    vote_lag = dplyr::lag(vote, order_by = election_year)
  ) %>% 
  ungroup() 

accountability %<>% 
  group_by(
    cod_ibge_6,
    election_year
  ) %>% 
  mutate(
    total_vote = sum(vote, na.rm = T),
    n_candidate = n()
  ) %>% 
  ungroup() %>% 
  filter(
    incumbent == 1,
    n_candidate > 1 # drop elections with only 1 candidate
  ) %>% 
  left_join(
    saeb_election,
    by = c("cod_ibge_6", "election_year")
  ) %>% 
  mutate(
    state = str_sub(cod_ibge_6, 1, 2),
    election_year = as.factor(election_year),
    gender = if_else(gender == 2, "male", "female"),
    elected = if_else(outcome == "eleito", 1, 0),
    vote_share = vote/total_vote*100
  ) %>% 
  left_join(censo)

finbra <- fread(
  here("data/finbra/finbra.csv")
)