# use school census data to produce set of tables
# 1) all municipal schools
# 2) all teachers
# 3) municipal classrooms.
# 4) all classrooms.
# 5) teacher turnover (municipal and all)
init_env <- ls()

censo_school <- read_data(
  "raw",
  "censo_escolar",
  "censo_school.csv.gz"
)

# keep only active schools and generate municipal table
censo_school_mun <- censo_school %>%
  filter(
    str_detect(active, "(1|2|ativ)")
  ) %>%
  group_by(
    cod_ibge_6,
    year,
    dep
  ) %>%
  summarise(
    n_school = n(),
    n_room = mean(rooms_existing),
    n_lower_sch = sum(
      elementary_sch,
      elementary_sch_8_9,
      elementary_sch_9,
      na.rm = T
    ),
    n_high_sch = sum(
      high_sch,
      high_sch_normal,
      na.rm = T
    ),
    prop_meal = mean(
      meal,
      na.rm = T
    ),
    prop_electricity = 1 - mean(
      electricity_inexistent,
      na.rm = T
    ),
    prop_internet = mean(
      internet,
      na.rm = T
    ),
    prop_kitchen = mean(
      kitchen,
      na.rm = T
    ),
    prop_sewer = 1 - mean(
      sewer_inexistent,
      na.rm = T
    ),
    prop_rural = mean(
      location == "rural",
      na.rm = T
    ),
    n_lab_info = sum(
      lab_info,
      na.rm = T
    ),
    n_library = sum(
      library,
      na.rm = T
    ),
    n_staff = sum(
      staff,
      na.rm = T
    ),
    n_teachers = sum(
      num_tchr,
      na.rm = T
    ),
    n_pta = sum(
      association_parents,
      na.rm = T
    ),
    n_tchr_unions = sum(
      association_tchrs,
      na.rm = T
    )
  ) %>%
  ungroup()

# fix entries
censo_school_mun <- censo_school_mun %>%
  mutate(
    n_pta = if_else(year == 2004, n_pta, NA_integer_),
    n_tchr_unions = if_else(year == 2004, n_tchr_unions, NA_integer_)
  )

censo_school_mun %>%
  write_data(
    "censo_escolar", "censo_school_mun.rds"
  )

censo_school %>%
  transmute(
    cod_ibge_6,
    year,
    dep,
    school_id,
    school_name,
    active,
    association_parents_tchrs,
    access_electricity = 1 - electricity_inexistent,
    access_water = 1 - water_inexistent,
    library,
    lab_info,
    meal,
    toilet
  ) %>%
  write_data(
    "censo_escolar",
    "censo_school.rds"
  )

reset_env(init_env)

# teacher aggregate stats by administration
# note: missing data for the year 2009, in the south
# missing data for rio grande do sul and santa catarina
teacher <- read_data(
  "raw",
  "censo_escolar",
  "censo_teacher_sample.csv.gz"
)

teacher_summary <- teacher %>%
  group_by(
    year,
    cod_ibge_6,
    dep,
    school_id
  ) %>%
  summarise(
    n_teacher = n_distinct(teacher_id),
    n_classes = n_distinct(classroom_id, na.rm = T),
    prop_higher_edu = mean(
      edu_desc == "higher edu complete",
      na.rm = T
    ),
    prop_commute = mean(
      cod_ibge_6 != str_sub(mun_residence, 1, 6),
      na.rm = T
    ),
    prop_female = mean(
      gender == "f",
      na.rm = T
    ),
    mean_age = mean(
      age,
      na.rm = T
    ),
    mean_n_class_per_teacher = mean(
      n_classes / n_teacher,
      na.rm = T
    )
  ) %>%
  ungroup()

teacher_summary %>%
  write_data(
    "censo_escolar",
    "censo_teacher_dep.rds"
  )

reset_env(init_env)

# student
censo_class <- read_data(
  "raw",
  "censo_escolar",
  "censo_class.csv.gz"
)

# add grade levels
censo_class <- censo_class %>%
  mutate(
    grade_level = case_when(
      cod_tching_stage < 3 ~ 0,
      cod_tching_stage == 4 | cod_tching_stage == 14 ~ 1, # lower school
      cod_tching_stage == 5 | cod_tching_stage == 15 ~ 2,
      cod_tching_stage == 6 | cod_tching_stage == 16 ~ 3,
      cod_tching_stage == 7 | cod_tching_stage == 17 ~ 4, # middle school
      cod_tching_stage == 8 | cod_tching_stage == 18 ~ 5,
      cod_tching_stage == 9 | cod_tching_stage == 19 ~ 6,
      cod_tching_stage == 10 | cod_tching_stage == 20 ~ 7,
      cod_tching_stage == 11 | cod_tching_stage == 21 ~ 8,
      cod_tching_stage == 22 | cod_tching_stage == 41 ~ 9,
      cod_tching_stage == 25 | cod_tching_stage == 30 | cod_tching_stage == 35 ~ 10, # high school
      cod_tching_stage == 26 | cod_tching_stage == 31 | cod_tching_stage == 36 ~ 11,
      cod_tching_stage == 27 | cod_tching_stage == 32 | cod_tching_stage == 37 ~ 12,
      cod_tching_stage == 28 | cod_tching_stage == 33 | cod_tching_stage == 38 ~ 13,
      T ~ NA_real_
    )
  )

# break down classroom by municipality
censo_class_mun <- censo_class %>%
  filter(dep == "municipal") %>%
  group_by(
    cod_ibge_6,
    year,
    grade_level
  ) %>%
  summarise_stats(
    num_enroll
  )

# break down classroom by department
censo_class_dep <- censo_class %>%
  group_by(
    dep,
    year,
    grade_level
  ) %>%
  summarise(
    total = sum(num_enroll)
  )

censo_class_dep %>%
  write_data(
    "censo_escolar",
    "censo_class_dep.rds"
  )

censo_class_mun %>%
  write_data(
    "censo_escolar",
    "censo_class_mun.rds"
  )

reset_env(init_env)

# teacher turnover
# aggregate
censo_teacher_turnover <- read_data(
  "raw",
  "censo_escolar",
  "censo_teacher_turnover.csv.gz"
)

censo_teacher_turnover <- censo_teacher_turnover %>%
  mutate_at(
    vars(
      starts_with("turnover"),
      starts_with("percent"),
      n
    ),
    as.numeric
  )

censo_turnover_school <- censo_teacher_turnover %>%
  calc_turnover(
    c("state", "cod_ibge_6", "year", "school_id", "grade_level", "location")
  )

censo_turnover_school %>%
  write_data(
    "censo_escolar",
    "censo_school_turnover.rds"
  )

censo_turnover_mun <- censo_teacher_turnover %>%
  calc_turnover(
    c("state", "cod_ibge_6", "year", "grade_level")
  )

censo_turnover_mun %>%
  write_data(
    "censo_escolar",
    "censo_mun_turnover.rds"
  )