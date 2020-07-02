# set-up ------------------------------------------------------------------
source(
  here::here("scripts/thesis_setup.R")
)

source(
  here("scripts/thesis_funs.R")
)

ggsave <- partial(
  ggplot2::ggsave,
  width = 4,
  height = 2.5
)

# data --------------------------------------------------------------------
saeb_hierarchical <- fread(
  here("data/saeb/saeb_hierarchical.csv.gz")
) 

censo_turnover <- fread(
  here('data/censo_escolar/censo_school_turnover.csv.gz')
) %>% 
  filter(
    grade_level %in% c(5, 9)
  )

censo_turnover %<>% 
  join_covariate

# remove small schools
saeb_hierarchical %<>% 
  filter(n_teacher >= 5)

saeb_hierarchical %<>%
  mutate_if(
    is.double,
    ~scale(.) %>% 
      as.vector
  )

# prep data ---------------------------------------------------------------
formula_model <- as.formula(
  turnover_index ~ grade_exam + grade + saeb_principal_female + saeb_principal_education + 
    saeb_principal_experience +  saeb_principal_appointment + education_teacher + type_contract_teacher + 
    saeb_wage_teacher + year_as_teacher + parent_attend_college_student + 
    parents_school_meeting_student + toilet + meal +
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate + year + state
)

model_hierarchical <- model.frame(
  formula_model,
  saeb_hierarchical
) %>% 
  as_tibble


# visualization -----------------------------------------------------------
model_hierarchical %>% 
  sample_n(2e3) %>% 
  ggplot(
    aes(
      turnover_index,
      grade_exam,
      color = grade,
      group = grade
    )
  ) +
  geom_point(
    alpha = 0.1
  ) +
  geom_smooth(
    method = "lm",
    color = "orange"
  ) 

model_hierarchical %>% 
  gg_histogram(
    aes(turnover_index)
  )

# covariate balance -------------------------------------------------------
formula_weight <- as.formula(
  turnover_index ~ grade + saeb_principal_female + saeb_principal_education + 
    saeb_principal_experience +  saeb_principal_appointment + education_teacher + type_contract_teacher + 
    saeb_wage_teacher + year_as_teacher + parent_attend_college_student + 
    parents_school_meeting_student + toilet + meal +
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate
)

weight_out <- weightit(
  formula_weight,
  model_hierarchical,
  method = 'ps'
)

bal.tab(
  weight_out
)

love.plot(
  weight_out, 
  threshold = .05, 
  abs = T,
  var.order = "unadjusted", 
  line = T
)

# hierarchical model ------------------------------------------------------
mods <- list()

formula_hlm <- as.formula(
  grade_exam ~ saeb_principal_female + saeb_principal_education + saeb_principal_experience +
    saeb_principal_appointment + education_teacher + type_contract_teacher + saeb_wage_teacher + year_as_teacher +
    parent_attend_college_student + parents_school_meeting_student + toilet + meal +
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate
)

mods[['lm']] <- lm(
  update(
    formula_hlm, ~ turnover_index*as.factor(grade) + . + as.factor(year) + as.factor(state)
  ),
  data = model_hierarchical,
  weights = weight_out$weights
)

mods[['lmer']] <- lmer(
  update(
    formula_hlm, ~ (turnover_index*as.factor(grade)) + . + (1|year) + (1|state)
  ),
  data = saeb_hierarchical
)

estimate_hlm <- map2_dfr(
  mods,
  names(mods),
  ~tidyfit(
    .x
  ) %>%
    mutate(
      type = .y
    )
)

plot_hlm <- estimate_hlm %>% 
  ggplot(
    aes(
      y = term,
      x = estimate,
      color = type,
      group = type
    )
  ) +
  geom_point(
    position = ggstance::position_dodgev(height=0.3)
  ) +
  geom_errorbar_tidy +
  theme(
    legend.position = c(0.9, 0.9),
    legend.title = element_blank()
  ) +
  geom_vline(xintercept = 0) +
  scale_y_discrete(
    limits = c(
      "turnover_index",
      "saeb_principal_experienceless than 2 years",
      "saeb_principal_experiencemore than 10",
      "year_as_teachermore than 10", 
      "year_as_teacherless than 2"
    ),
    labels = c(
      "turnover_index" = "Teacher turnover",
      "saeb_principal_experienceless than 2 years" = "Principal: less than 2 years of exp.",
      "saeb_principal_experiencemore than 10" = "Principal: over 10 years of exp.",
      "year_as_teachermore than 10" = "Teacher: over 10 years of exp.", 
      "year_as_teacherless than 2" = "Teacher: less than 2 years of exp."
    )
  ) 

# robustness check --------------------------------------------------------
lm_weight <- function(data, index){
  weight_out <- weightit(
    formula_weight,
    data = model_hierarchical[index,],
    method = 'ps'
  )
  
  fit <- lm(
    update(
      formula_hlm, ~ turnover_index*as.factor(grade) + . + as.factor(year) + as.factor(state)
    ),
    data = model_hierarchical[index,],
    weights = weight_out$weights
  )
  
  return(coef(fit)['turnover_index'])
}

boot_lm <- boot(
  lm_weight, 
  data = model_hierarchical, 
  R = 100,
  parallel = "multicore", 
  ncpus = 6
)

# ggsave(
#   plot_hlm,
#   filename = here(path_figs, "turnover_fit.pdf")
# )