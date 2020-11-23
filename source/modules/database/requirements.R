# functions ---------------------------------------------------------------
run_task <- function(task) {
  print(paste("running", task, "..."))

  source(here("scripts/", "tasks", task))

  print(
    paste(task, "complete!")
  )
}

mutate_high_bureaucrat <- function(data) {
  data_high_bureaucrat <- data %>%
    mutate(
      high_bureaucrat = if_else(occupation == 1 & municipal == 1, 1, 0)
    )

  return(data_high_bureaucrat)
}

mutate_high_bureaucrat_lag <- function(data, .group_vars) {
  data_lag <- data %>%
    group_by(
      across({{ .group_vars }})
    ) %>%
    mutate(
      high_bureaucrat_lag = lag(high_bureaucrat, default = 0, order_by = year)
    ) %>%
    ungroup()

  return(data_lag)
}

predict_wage <- function(model, data) {
  wage <- predict(model, newdata = data)
  exp_wage <- exp(wage)

  return(exp_wage)
}

year_to_char <- function(data) {
  data <- data %>%
    mutate_at(
      vars(year),
      as.character
    )

  return(data)
}

fix_wage <- function(data) {
  data %>%
    mutate(
      wage = wage * 0.573395 / (cpi / 100)
    ) %>%
    select(-cpi)
}

fix_contract <- function(data) {
  data %>%
    mutate(
      contract_type = case_when(
        contract <= 25 ~ "regular",
        between(contract, 30, 35) ~ "career",
        between(contract, 40, 50) ~ "temporary",
        TRUE ~ "other"
      )
    )
}

fix_occupation <- function(data) {
  data %>%
    mutate(
      cbo_group = recode(
        occupation,
        `1` = "bureaucrat_high",
        `2` = "frontline_high",
        `3` = "frontline_high",
        `4` = "bureaucrat_low",
        `5` = "frontline_low",
        .default = "other"
      ),
      cbo_group_detail = recode(
        occupation,
        `0` = "army",
        `1` = "executive",
        `2` = "liberal arts",
        `3` = "engineer",
        `4` = "administration",
        `5` = "services",
        `6` = "agriculture",
        `7` = "industry",
        `8` = "industry",
        `9` = "maintenance"
      )
    )
}

trim_rais <- function(data, ...) {
  data %>%
    transmute(
      id_employee,
      year,
      cod_ibge_6 = municipio,
      cbo_02 = cbo2002,
      cnae = cnae20classe,
      age,
      edu,
      gender = genero,
      race = racacor,
      work_experience = tempoempr,
      wage = vlremmedia,
      hired = if_else(tipoadmissao == 1 | tipoadmissao == 2, 1, 0),
      fired = if_else(causadesli == 10 | causadesli == 11, 1, 0),
      departure = if_else(causadesli == 20 | causadesli == 21, 1, 0),
      date_admission,
      date_separation,
      contract = tipovinculo,
      nat_jur = naturjur,
      municipal = if_else(naturjur == 1031, 1, 0),
      outcome = recode(fired, `0` = 0, `10` = 2, `11` = 2, .default = 1),
      state = substr(cod_ibge_6, 1, 2),
      cbo_02 = ifelse(nchar(cbo_02) == 5, paste0("0", cbo_02), cbo_02),
      occupation = as.integer(substr(cbo_02, 1, 1)),
      hours = horascontr,
      cpi,
      ...
    )
}

make_dummy <- function(data, var) {
  dummy <- enquo(var)

  data %>%
    mutate(
      indicator = 1,
      row = row_number()
    ) %>%
    pivot_wider(
      names_from = !!dummy,
      names_prefix = paste0(rlang::as_name(dummy), "_"),
      values_from = indicator,
      values_fill = list(indicator = 0)
    ) %>%
    select(-row) %>%
    return()
}

is_bureau <- rlang::quo(nat_jur == 1031 & occupation == 1)

filter_bureaucrat <- function(data) {
  data_bureaucrats <- data %>%
    filter(nat_jur == 1031 & occupation == 1)

  return(data_bureaucrats)
}

exclude_id_null <- function(data) {
  data_out <- data %>%
    filter(
      sql("id_employee IS NOT NULL")
    )

  return(data_out)
}

filter_exit <- function(data, type) {
  type <- sym(type)

  data_out <- data %>%
    filter(
      {{ type }} == 1
    )
}

filter_first_year <- function(data, .group_vars) {
  data_first_year <- data %>%
    group_by(
      across({{ .group_vars }})
    ) %>%
    filter(
      year == min(as.numeric(year))
    ) %>%
    ungroup()

  return(data_first_year)
}

filter_bureau_only_mun <- function(data) {
  temp <- data %>%
    group_by(id_employee) %>%
    mutate(
      n = n(),
      non_municipal = sum(municipal != 1, na.rm = T)
    ) %>%
    ungroup() %>%
    filter(
      non_municipal == 0,
      n == 1
    ) %>%
    select(-n, -non_municipal)

  return(temp)
}

extract_employee_id <- function(data) {
  temp <- data %>%
    distinct(id_employee)

  return(temp)
}

# rais_panel_extract <- function(file, is_sample = F) {
#   n_row <- ifelse(is_sample, 1e6, Inf)
#   t0 <- str_extract(file, "\\d{4}")
#   t1 <- as.character(as.numeric(t0) + 1)

#   # rais_t0 <- read_dta(file, n_max = n_row)

#   rais_bureau_t0 <- fread(file) %>%
#     extract_bureau_id() %>%
#     mutate(
#       bureau_last_year = 1
#     )

#   rais_t1 <- fread(
#     str_replace(file, t0, t1)
#   ) %>%
#     left_join(rais_bureau_t0) %>%
#     filter(
#       !!is_bureau | bureau_last_year == 1
#     ) %>%
#     trim_rais(bureau_last_year) %>%
#     replace_na(
#       list(bureau_last_year = 0)
#     ) %>%
#     fix_wage()

#   gc()

#   return(rais_t1)
# }

fread <- purrr::partial(
  data.table::fread,
  nThread = parallel::detectCores()
)

read_rais <- function(year) {
  data <- fread(
    here(sprintf("data/rais_%s.csv.gz", year)),
    select = c(
      "id_employee",
      "year",
      "municipio",
      "cbo2002",
      "vlremmedia",
      "tipoadmissao",
      "causadesli",
      "date_admission",
      "date_separation",
      "tipovinculo",
      "naturjur"
    )
  )
}

filter_bureau_year <- function(data, year) {
  t <- as.character(year)

  filter(
    data,
    cbo_group == "bureaucrat_high",
    municipal == 1,
    year == t
  ) %>%
    return()
}

filter_one_job <- function(data) {
  data %>%
    add_count(id_employee) %>%
    filter(n == 1) %>%
    return()
}

calc_age <- function(from, to) {
  from_lt <- as.POSIXlt(from)
  to_lt <- as.POSIXlt(to)

  age <- to_lt$year - from_lt$year

  age <- ifelse(
    to_lt$mon < from_lt$mon |
      (to_lt$mon == from_lt$mon & to_lt$mday < from_lt$mday),
    age - 1, age
  )

  return(age)
}

# stargazer <- partial(
#   stargazer::stargazer,
#   font.size = "small",
#   float = F,
#   no.space = T
# )

print_log <- function(file, text = "upload to sql") {
  year <- str_extract(file, "\\d{4}")

  msg <- paste(text, year, "started.")
  print(msg)
}

write_log <- function(file, text = "upload to sql", filepath = here("log.txt")) {
  year <- str_extract(file, "\\d{4}")
  msg <- paste(text, year, "completed.")

  print(msg)

  cat(
    msg,
    file = filepath,
    append = T
  )
}

print_section <- function(text) {
  print(
    paste("estimating", text)
  )
}

sample_sql <- function(tbl, n) {
  tbl <- tbl %>%
    mutate(random = random()) %>%
    arrange(random) %>%
    head(n) %>%
    select(-random)

  return(tbl)
}

count_distinct <- function(data, var) {
  var <- enquo(var)

  data %>%
    distinct(!!var) %>%
    nrow() %>%
    return()
}

tbl_nrow <- function(tbl) {
  tbl %>%
    summarise(n = n())
}

grouped_lead <- function(data, .group_vars, .lag_vars, .ordering) {
  data_lead <- data %>%
    group_by(
      across({{ .group_vars }})
    ) %>%
    mutate(
      across(
        {{ .lag_vars }},
        ~ lead(., order_by = {{ .ordering }}),
        .names = "future_{col}"
      )
    ) %>%
    ungroup()

  return(data_lead)
}

compute_mean <- function(data, ...) {
  data_summarised <- data %>%
    summarise(
      across(
        c(...),
        ~ mean(., na.rm = T),
        .names = "mean_{col}"
      ),
      .groups = "drop"
    )

  return(data_summarised)
}

compute_median <- function(data, ...) {
  data_summarised <- data %>%
    summarise(
      across(
        c(...),
        ~ median(., na.rm = T),
        .names = "median_{col}"
      ),
      .groups = "drop"
    )

  return(data_summarised)
}

pluck_reg <- function(oaxaca, group) {
  reg <- fit_oaxaca %>%
    pluck(
      "reg",
      paste0("reg.", group)
    )

  return(reg)
}

fm <- function(dv, predictor, ...) {
  controls <- enquos(...) %>%
    purrr::map_chr(
      rlang::as_label
    )

  covariate <- paste0(
    c(substitute(predictor), controls),
    collapse = "+"
  )

  fm <- as.formula(
    paste(substitute(dv), covariate, sep = "~")
  )

  return(fm)
}

plot_summary_stats <- function(data, dependent_variable, title) {
  plot <- data %>%
    mutate(
      year = as.numeric(year)
    ) %>%
    ggplot(
      aes_string("year", dependent_variable)
    ) +
    geom_point() +
    geom_line() +
    # geom_hline(yintercept = 1, lty = "dotted") +
    geom_vline(xintercept = c(2009, 2013), lty = "dotted") +
    scale_x_continuous(breaks = seq(2007, 2015, 2)) +
    # scale_y_continuous(labels = scales::percent) +
    labs(
      x = "Year",
      y = "Outcome"
    ) +
    # coord_cartesian(ylim = c(0.8, 1.6)) +
    ggtitle(title) +
    theme(
      text = element_text(size = 8),
      plot.title = element_text(size = 9)
    )

  return(plot)
}

scale_vars_to_baseline <- function(data, vars, baseline_year) {
  data_out <- data %>%
    mutate(
      across(vars, function(var) var / var[year == baseline_year])
    )

  return(data_out)
}