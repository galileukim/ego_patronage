# ==============================================================================
# run modules
# ==============================================================================
source(
  here("build", domain, "setup.R")
)

# run tasks
modules <- c(
  "visual_map"
  "visual_global_edu",
  "visual_budget",
  "visual_bureaucracy",
  "model_hierarchical",
  "model_saeb"
  )

walk(
  modules,
  ~run_module(.)
)

# turnover spaece ---------------------------------------------------------
plot_spaece <- censo_turnover_score %>% 
  filter(
    n >= 5
  ) %>% 
  mutate(
    grade_level = as.factor(grade_level)
  ) %>% 
  ggplot(
    aes(
      turnover_index,
      spaece_mean,
      group = as.factor(grade_level),
      col = as.factor(grade_level)
    )
  ) +
  geom_smooth(
    method = lm, formula = y ~ splines::bs(x, 3),
    se = F
  ) +
  annotate(
    "text",
    label = "2nd grade",
    size = 2,
    col = "grey65",
    0.45, 165
  ) +
  annotate(
    "text",
    label = "5th grade",
    size = 2,
    col = "grey65",
    0.6, 200
  ) +
  annotate(
    "text",
    label = "9th grade",
    size = 2,
    col = "grey65",
    0.7, 225
  ) +
  labs(
    x = "Teacher turnover (index)",
    y = "Test scores (SPAECE)"
  ) +
  theme(
    legend.position = "none"
  )

ggsave(
  plot_spaece,
  filename = p_file_here('figs', "turnover_spaece.pdf")
)

# explaining turnover -----------------------------------------------------
# show that irrespective of level of economic development, the same dynamic is present
# turnover index on coalition
saeb_turnover <- censo_school_turnover %>% 
  join_covariate() %>% 
  left_join(
    censo_school %>% 
      mutate(
        school_id = as.integer(school_id)
      ) %>% 
      select(-state, -location),
    by = c('cod_ibge_6', 'year', 'school_id')
  )

f_turnover <- c(
  as.formula(turnover_index ~ coalition_share + as.factor(year) + as.factor(state)),
  as.formula(
    turnover_index ~ coalition_share + as.factor(year) + as.factor(state) + 
      location +
      toilet + internet + meal +
      censo_log_pop + censo_median_wage + censo_rural
  )
)

fit_turnover <- map(
  f_turnover,
  ~lm(., data = saeb_turnover)
)

# logit model
f_logit <- fm(
  rais_hired,
  coalition_share,
  rais_category,
  election_year,
  state,
  rais_edu,
  rais_wage, 
  censo_log_pop, 
  mayor_reelected,
  censo_median_wage, 
  mayor_campaign, 
  mayor_coalition_size
)

fit_turnover <- c(
  f_logit,
  update(f_logit, rais_fired ~ .)
) %>% 
  map(
    ~logit(., data = rais_fit)
  )
  
plot_model(
  fit_hired,
  type = 'pred',
  terms = c('coalition_share [all]')
) +
  coord_cartesian(
    ylim = c(0.05, 0.07)
  )

plot_model(
  fit_fired,
  type = 'pred',
  terms = c('coalition_share [all]')
)

plot_logit_int <- map(
  c(
    'rural' = 'censo_rural[0.25, 0.5, 0.75]', 
    'population' = 'censo_pop[1e4, 5e4, 1e5]',
    'wage' = 'censo_median_wage [172.12, 250.77, 341.896]'
  ),
  ~plot_model(fit_hired, type = 'pred', title = NULL, terms = c('coalition_share [all]', .))
)

walk2(
  plot_logit_int,
  names(plot_logit_int),
  ~ggsave(
    .x,
    filename = p_file_here('figs', paste0('plot_logit_', .y, '.pdf'))
  )
)

# print plot
plot_logit <- fit_hired %>%
  tidycoef(
    vars = "coalition|mayor_reelected|censo_log_pop|censo_median_wage"
  ) +
  theme(
    axis.title = element_blank()
  )

ggsave(
  plot_logit,
  filename = p_file_here('figs', "fit_logit.pdf")
)

# municipal fe model
fit_hired_mun <- fit_felm(
  fit_hired_mun,
  "rais_hired",
  predictor = c("coalition_share + as.factor(rais_category) + mayor_reelected"),
  control = controls,
  data = rais_edu_mun %>% 
    mutate(
        rais_category = fct_relevel(rais_category, 'teacher')
    )
)

fit_fired_mun <- fit_felm(
  fit_hired_mun,
  "rais_fired",
  predictor = c("coalition_share + as.factor(rais_category) + mayor_reelected"),
  control = controls,
  data = rais_edu_mun %>% 
    mutate(
      rais_category = fct_relevel(rais_category, 'teacher')
    )
)

# table of results
sink(p_file_here('results', 'turnover_result.tex'))
mstar(
  list(
    fit_turnover[[2]],
    fit_hired,
    fit_fired,
    fit_hired_mun,
    fit_fired_mun
  ),
  keep = c('coalition_share', 'rais_category'),
  add.lines = list(c("Controls", rep("\\checkmark", 5))),
  covariate.labels = c('Executive share of seats', 'School principal', 'Executive share of seats X School principal'),
  dep.var.labels.include = F,
  column.labels = c('Turnover index', 'Hired (Logit)', 'Fired (Logit)', 'Hired (FE)', 'Fired (FE)'),
  column.separate = rep(1, 5),
  model.names = FALSE
)
sink()

plot_mun <- fit_hired_mun%>% 
  tidycoef(
    vars = "coalition|mayor_reelected|censo_log_pop|censo_median_wage"
  ) +
  # scale_y_discrete(
  #   labels = rev(c("Share of legislative seats", "Population size (logged)", "Median wage", "Size of coalition", "Reelected", "Share of legislative seats (sq.)"))
  # ) +
  theme(
    axis.title = element_blank()
  )

ggsave(
  plot_mun,
  filename = p_file_here('figs', "fit_mun.pdf")
)

plot_hired <- plot_grid(
  plot_logit,
  plot_mun,
  nrow = 2,
  labels = c("Logit", "OLS"),
  label_size = 7.5
)

ggsave(
  plot_hired,
  filename = p_file_here('figs', "hired_fit.pdf")
)

# predicted vals
model_fit_hired_mun <- model.frame(
  fit_hired_mun
) 

plot_hired <- model_fit_hired_mun %>% 
  mutate(
    predict = fit_hired_mun$fitted.values
  ) %>% 
  group_by(coalition_share) %>% 
  mutate(predict_mean = mean(rais_hired, na.rm = T)) %>% 
  ggplot(
    aes(x = coalition_share)
  ) +
  geom_count(
    aes(y = predict_mean),
    alpha = 0.1
  ) +
  geom_smooth(
    aes(y = predict),
    method = lm,
    formula = y ~ x,
    col = "coral3"
  ) +
  coord_cartesian(
    ylim = c(0.05, 0.125)
  ) +
  labs(
    x = "Share of legislative seats",
    y = "Proportion of new hires"
  ) +
  theme(legend.position = 'none')

model_fit_fired_mun <- model.frame(
  fit_fired_mun
) 

plot_fired <- model_fit_fired_mun %>% 
  mutate(
    predict = fit_fired_mun$fitted.values
  ) %>% 
  group_by(coalition_share) %>% 
  mutate(predict_mean = mean(rais_fired, na.rm = T)) %>% 
  ggplot(
    aes(x = coalition_share)
  ) +
  geom_count(
    aes(y = predict_mean),
    alpha = 0.1
  ) +
  geom_smooth(
    aes(y = predict),
    method = lm,
    formula = y ~ x,
    col = "coral3"
  ) +
  coord_cartesian(
    ylim = c(0, 0.075)
  ) +
  labs(
    x = "Share of legislative seats",
    y = "Proportion of dismissals"
  ) +
  theme(legend.position = 'none')

plot_turnover <- grid.arrange(
  grobs = list(
    plot_hired,
    plot_fired
  ),
  ncol = 2
)

ggsave(
  plot_turnover,
  filename = p_file_here('figs', "plot_pred.pdf")
)

# accountability ----------------------------------------------------------
fit_acc <- list()

fit_acc$lm <- lm(
  vote_share ~ delta_mean_grade_exam + edu_desc + gender + occupation + 
    as.factor(party) + vote_lag + delta_mean_grade_exam + 
    censo_rural + censo_log_pop + censo_student_age,
  data = accountability
)

fit_acc$felm <- fit_felm(
  fit_acc,
  "vote_share",
  predictor = "delta_mean_grade_exam",
  control = c(
    "edu_desc", "gender", "occupation", "as.factor(party)", "vote_lag", 
    "delta_mean_grade_exam", "censo_rural", "censo_log_pop", "censo_student_age"
  ),
  data = accountability,
  cluster = c("state + election_year")
)

plot_accountability <- fit_acc %>% 
  map2_df(
    names(.),
    ~tidy(.x, conf.int = T) %>% 
      filter(
        str_detect(
          term,
          "delta|censo_log_pop|censo_rural|gender|vote_lag"
        )
      ) %>% 
      mutate(
        name = .y
      )
  )

plot_accountability <- ggplot(
  data = plot_accountability,
  mapping = aes(
    y = term, 
    x = estimate,
    color = name,
    group = name
  ),
  conf.int = T
) +
  geom_point(
    position = ggstance::position_dodgev(height=0.3)
  ) +
  geom_errorbar_tidy

ggsave(
  plot_accountability,
  filename = p_file_here('figs', "accountability_fit.pdf")
)

# covariate balance -------------------------------------------------------
weight_out <- weightit(
  turnover_index ~ saeb_principal_female + saeb_principal_education + saeb_principal_appointment +
    education_teacher + saeb_wage_teacher + year_as_teacher + 
    parent_attend_college_student + parents_school_meeting_student + fridge_student + failed_school_year_student +
    toilet + meal +
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate,
  data = saeb_hierarchical %>% 
    filter(!is.na(turnover_index)),
  method = 'ps',
  missing = 'ind'
)

love.plot(
  weight_out,
  threshold = .1, 
  abs = TRUE,
  var.order = "unadjusted",
  line = TRUE
)

ggsave(
  filename = p_file_here('figs', 'balance_hlm.pdf')
)

weight_out <- weightit(
  coalition_share ~rais_permanent + rais_size + I(budget_education/censo_pop) + 
    censo_median_wage+ censo_log_pop+ mayor_edu,
  data = rais_edu_mun %>% 
    filter(!is.na(coalition_share)),
  method = 'ps',
  missing = 'ind'
)

love.plot(
  weight_out,
  threshold = .1, 
  abs = TRUE,
  var.order = "unadjusted",
  line = TRUE
)

ggsave(
  filename = p_file_here('figs', 'balance_felm.pdf')
)

# rdd ---------------------------------------------------------------------
formula_rdd <- fm(
  turnover_index,
  quotient_margin,
  cod_ibge_6,
  mayor_opposition_flip,
  state,
  year
)

censo_mun_turnover <- censo_school_turnover %>% 
  filter(
    n >= 10,
    between(grade_level, 5, 9),
    year %in% seq(2005, 2013, 4) # first year of mandate
  ) %>% 
  group_by(
    state,
    cod_ibge_6,
    year
  ) %>% 
  summarise(
    turnover_index = weighted.mean(turnover_index, n)
  ) %>% 
  ungroup()

censo_mun_turnover %<>% 
  add_election %>% 
  left_join(
    quotient,
    by = c("cod_ibge_6", "election_year")
  )

censo_turnover_model <- model.frame(
  formula_rdd,
  censo_mun_turnover
) %>% 
  join_covariate() %>% 
  mutate_at(
    vars(mayor_opposition_flip),
    ~dplyr::recode(., `0` = 'placebo', `1` = 'mayor_opposition_flip')
  )

turnover <- censo_turnover_model %>% 
  group_split(mayor_opposition_flip) %>% 
  map(
    pull,
    turnover_index
  )
  
quotient_margin <- censo_turnover_model %>% 
  group_split(mayor_opposition_flip) %>%
  map(
    pull,
    quotient_margin
  )

covariates <- censo_turnover_model %>% 
  dplyr::group_split(mayor_opposition_flip) %>% 
  map(
    ~select(., state, year)
  )

rdd_plots <- pmap(
  list(
    turnover = turnover,
    quotient_margin = quotient_margin,
    names = names(turnover)
  ),
  function(turnover, quotient_margin, names){
    rdplot(turnover, quotient_margin, title = paste('RDD:', names), y.lim = c(0.4, 0.8))
  }
)

rdd_turnover <- pmap(
  list(
    x = turnover,
    y = quotient_margin,
    z = covariates
  ),
  function(x, y, z){rdrobust(x, y, covs = z)}
)
  
lm_turnover <- censo_turnover_model %>% 
  mutate(
    treat = if_else(quotient_margin > 0, 1, 0)
  ) %>% 
  filter(
    abs(quotient_margin) <= 0.005
  ) %>% 
  group_split(mayor_opposition_flip) %>% 
  map(
    ~lm(
      turnover_index ~ quotient_margin*treat + as.factor(year) + as.factor(state),
      data = .
    )
  )

censo_turnover_model %>% 
  ggplot(
    aes(quotient_margin, turnover_index)
  ) +
  stat_summary_bin(
    fun = 'mean',
    geom = 'point',
    binwidth = 0.0005,
    orientation = 'x',
    alpha = 0.5
  ) +
  geom_vline(
    xintercept = 0,
    linetype = 'dashed'
  ) +
  facet_wrap(
    . ~ mayor_opposition_flip,
    nrow = 2
  )

# bottom line: there is no effect of gaining an additional seat for the mayor on patronage.

# ivreg -------------------------------------------------------------------
fit_iv <- AER::ivreg(
  grade_exam ~ turnover_index + saeb_principal_female + saeb_principal_education + saeb_principal_appointment +
    education_teacher + saeb_wage_teacher +
    parent_attend_college_student + parents_school_meeting_student + fridge_student + failed_school_year_student +
    toilet + meal + 
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate| 
    coalition_share + saeb_principal_female + saeb_principal_education + saeb_principal_appointment +
    education_teacher + saeb_wage_teacher + 
    parent_attend_college_student + parents_school_meeting_student + fridge_student + failed_school_year_student +
    toilet + meal + 
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate,
  data = saeb_hierarchical
)

fit_iv_2 <- AER::ivreg(
  grade_exam ~ turnover_index + saeb_principal_female + saeb_principal_education + saeb_principal_appointment +
    education_teacher + saeb_wage_teacher +
    parent_attend_college_student + parents_school_meeting_student + fridge_student + failed_school_year_student +
    toilet + meal + 
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate| 
    first_year + saeb_principal_female + saeb_principal_education + saeb_principal_appointment +
    education_teacher + saeb_wage_teacher + 
    parent_attend_college_student + parents_school_meeting_student + fridge_student + failed_school_year_student +
    toilet + meal + 
    censo_median_wage + censo_log_pop + censo_rural + censo_lit_rate,
  data = saeb_hierarchical %>% 
    mutate(first_year = if_else(year == election_year + 1, 1, 0))
)

# patronage and reelection ------------------------------------------------
patronage_reelection <- patronage_reelection %>% 
  # filter(cbo_category == 'education') %>% 
  mutate_at(
    vars(election_year, state),
    as.factor
  ) %>% 
  mutate(
    mayor_coalition_member = fct_relevel(mayor_coalition_member, "non_coalition_member")
  )

plot_patronage <- patronage_reelection %>%
  gg_summary(
    prop_hired,
    reelected,
    smooth = T
  ) +
  facet_wrap(
    type ~ .
  ) +
  coord_cartesian(ylim = c(0.4, 0.7)) +
  theme_bw() +
  theme_clean

pdf(
  p_file_here('figs', 'patronage.pdf'),
  width = 5,
  height = 3
)
plot_patronage
dev.off()

# estimation
f_reelection <- fm(
  dv = reelected, 
  predictor = prop_hired,
  state, election_year,
  campaign,
  edu, gender, party_fct,
  censo_rural, censo_median_wage, censo_log_pop
)

fit_reelection_mayor <- glm(
  f_reelection,
  family = 'binomial',
  data = patronage_reelection %>% 
    filter(position == 'prefeito')
)

f_reelection_councilor <- update(f_reelection, . ~ prop_hired*mayor_coalition_member + .)

fit_reelection_councilor <- glm(
  f_reelection_councilor,
  family = 'binomial',
  data = patronage_reelection %>% 
    filter(position == 'vereador')
)

marginal_mayor <- margins(
  fit_reelection_mayor,
  variables = 'prop_hired'
)

# marginal_councilor <- margins(
#   fit_reelection_councilor,
#   variables = 'prop_hired',
#   at = list(
#     mayor_coalition_member = c('coalition_member', 'non_coalition_member')
#   )
# )
# 
# write_rds(
#   marginal_councilor,
#   p_file_here('results', 'marginal_effects_councilor.rds'),
#   compress = 'gz'
# )

marginal_councilor <- read_rds(
  p_file_here('results', 'marginal_effects_councilor.rds')
)

ame <- list(
  mayor = marginal_mayor,
  councilor = marginal_councilor
) %>% 
  map_dfr(
    ~summary(.),
    .id = 'type'
  ) %>% 
  mutate(
    type = if_else(
      type == 'mayor', 
      'mayor', 
      as.character(mayor_coalition_member)
    ) %>% 
      fct_relabel(~str_replace_all(., "_", " ")) %>% 
      fct_relevel(
        "mayor"
      )
  )

plot_ame <- ame %>% 
  ggplot() + 
  geom_linerange(
    aes(
      x = type,
      ymin = lower,
      ymax = upper
    ),
    color = matte_indigo
  ) +
  geom_point(
    aes(type, AME),
    size = 3
  ) + 
  geom_hline(
    yintercept = 0,
    linetype = 'dashed',
    color = 'gray55'
  ) + 
  coord_cartesian(ylim = c(-0.025, 0.04))

ggsave(
  p_file_here(
    'figs', 'patronage_ame.pdf'
  ),
  plot_ame
)

# vote share
patronage_reelection <- patronage_reelection %>%
  group_by(
    cod_ibge_6, position, election_year
  ) %>% 
  mutate(
    total_vote = sum(vote)
  ) %>% 
  ungroup() %>% 
  mutate(
    vote_share = vote/total_vote
  )

f_vote <- update(
  f_reelection,
  vote_share ~ prop_hired*mayor_coalition_member + .
)

lm_councilor <- lm(
  f_vote,
  data = patronage_reelection %>% 
    filter(position == 'vereador')
)

plot_model(
  lm_councilor,
  type = 'pred',
  terms = c('prop_hired', 'mayor_coalition_member[coalition_member, non_coalition_member]')
)

patronage_reelection %>% 
  filter(elected == 1) %>% 
  group_by(position) %>% 
  summarise(
    vote = sum(vote)
  )
