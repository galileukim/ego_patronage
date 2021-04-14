here_data <- partial(
    here,
    data_path
)
here_source <- partial(
    here,
    source_path
)

fread <- purrr::partial(
  data.table::fread,
  nThread = parallel::detectCores(),
  nrows = nrows
)

dbExecute <- purrr::partial(
  DBI::dbExecute,
  conn = rais_con
)

dbGetQuery <- purrr::partial(
  DBI::dbGetQuery,
  conn = rais_con
)

nrows <- if (isTRUE(debug)) 1e4 else Inf

# ==============================================================================
# data io
# ==============================================================================
here_data <- function(type, dir, file) {
  path <- here("data", type, dir, file)
  
  return(path)
}

read_data <- function(type, dir, file) {
  file_path <- here_data(type, dir, file)

  if (str_detect(file, ".rds$")) {
    data <- read_rds(file_path)
  } else {
    data <- fread(file_path)
  }
  
  return(data)
}

get_data <- function(type, dir, file, cols){
  data <- read_data(type, dir, file)

  data_subset <- select(data, all_of(cols))

  return(data_subset)
}

write_data <- function(object, dir, file, type = "clean", compress = "gz") {
  file_path <- here_data(type, dir, file)
  
  create_dir(dir)

  if (str_detect(file, ".rds$")) {
    write_rds(object, file_path, compress = compress)
  } else {
    fwrite(object, file_path, compress = "gzip")
  }
}

create_dir <- function(dir){
  if(!dir.exists(dir)){
    dir.create(dir, recursive = TRUE)
  }
}

reset_env <- function(init_env){
  final_env <- ls(.GlobalEnv)

  rm(
    envir = .GlobalEnv,
    list = setdiff(final_env, init_env)
  )

  gc()
}

build_repo <- function(module){
  repo <- here("data", "clean", module)

  if(!dir.exists(repo)){
    dir.create(repo)
  }else{
    unlink(repo, recursive = T)
    dir.create(repo)
    }
  }

run_module <- function(module, domain) {
  print(
    paste0("running module ", module, "...")
  )

  build_repo(module)

  source_file <- paste0("datagen_", module, ".R")
  path <- here("source", domain, "modules", source_file)

  init_env <- ls(.GlobalEnv)
  source(path)
  reset_env(init_env)

  print(
    paste("module", module, "complete!")
  )
}

fread <- purrr::partial(
  data.table::fread,
  nThread = parallel::detectCores(),
  integer64 = c("character")
)

list_files <- function(path, pattern) {
  files <- map2(
    path,
    pattern,
    list.files
  ) %>%
    flatten_chr()

  return(files)
}

write_tables_to_sql <- function(tables, names, conn, overwrite = TRUE){
  pwalk(
      list(
          name = names,
          value = tables
      ),
      dbWriteTable,
      conn = rais_con,
      overwrite = overwrite
  )
}

# ==============================================================================
# rais wrangling
# ==============================================================================
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
    ) %>%
    select(-contract)
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
    ) %>%
    select(-occupation)
}

fix_edu <- function(data){
  data %>%
  mutate(
            edu_category = case_when(
                edu <= 2 ~ "illiterate",
                between(edu, 3, 4) ~ "lower school",
                between(edu, 5, 6) ~ "middle school",
                between(edu, 7, 8) ~ "high school",
                edu >= 9 ~ "higher education"
            )
  )
}

create_municipal_dummy <- function(data){
  data %>%
    mutate(
      municipal = if_else(nat_jur == 1031, 1, 0)
    )
}

trim_rais <- function(data, ...) {
  data_trimmed <- data %>%
    select(
      id_employee,
      cod_ibge_6 = municipio,
      cbo_02 = cbo2002,
      matches("cnae_[0-9]{2}"),
      age,
      edu,
      gender = genero,
      race = racacor,
      work_experience = tempoempr,
      wage = vlremmedia,
      date_admission,
      date_separation,
      contract = tipovinculo,
      nat_jur = naturjur,
      hours = horascontr,
      cpi,
      type_admission = tipoadmissao,
      cause_fired = causadesli
    ) %>%
    mutate(
      year = as.integer(year),
      municipal = if_else(nat_jur == 1031, 1, 0),
      gender = if_else(gender == 1, "male", "female"),
      hired = if_else(type_admission == 1 | type_admission == 2, 1, 0),
      fired = if_else(cause_fired == 10 | cause_fired == 11, 1, 0),
      departure = if_else(cause_fired == 20 | cause_fired == 21, 1, 0),
      cbo_02 = ifelse(nchar(cbo_02) == 5, paste0("0", cbo_02), cbo_02),
      date_admission = as.character(date_admission, format = "%Y-%m-%d"),
      date_separation = as.character(date_separation, format = "%Y-%m-%d")
    ) %>%
    select(
      id_employee,
      cod_ibge_6,
      year,
      everything()
    )

  return(data_trimmed)
}

create_dummy <- function(data, var) {
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
      !is.na(id_employee) & id_employee != ""
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

filter_active_filiado <- function(data) {
  data %>%
    generate_year_filiado() %>%
    filter(
      year >= year_date_start &
        year <= year_date_end
    )
}

generate_year_filiado <- function(data) {
  data %>%
    mutate(
      across(
        c(date_start, date_end),
        ~ str_sub(., 1, 4) %>% as.integer(),
        .names = "year_{col}"
      )
    )
}

extract_employee_id <- function(data) {
  temp <- data %>%
    distinct(id_employee)

  return(temp)
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


compute_summary_stats <- function(data, .group_vars, .summary_vars){
  data_summarised <- data %>%
    group_by(
      across({{.group_vars}})
    ) %>%
    summarise(
      across(
        {{.summary_vars}},
        list(mean = mean, sd = sd),
        na.rm = TRUE,
        .names = "{col}_{fn}"
      ),
      .groups = "drop"
    )

  data_count <- data %>%
    group_by(
      across({{.group_vars}})
    ) %>%
    summarise(
      n = n(),
    .groups = "drop"
    )

  data_summary_stats <- data_summarised %>%
    inner_join(
      data_count,
      by = .group_vars
    )

  return(data_summary_stats)
}

compute_mean <- function(data, .group_vars, .summary_vars) {
  data_summarised <- data %>%
    group_by(
      across({{.group_vars}})
    ) %>%
    summarise(
      across(
        {{.summary_vars}},
        ~ mean(., na.rm = T),
        .names = "{col}_mean"
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
        .names = "{col}_median"
      ),
      .groups = "drop"
    )

  return(data_summarised)
}

classify_partisanship <- function(data){
  data %>%
    mutate(
        is_partisan = if_else(
            year >= year_date_start &
            (year <= year_date_end | is.na(year_date_end)),
            "pre_partisan", "post_partisan",
            missing = "non_partisan"
        ),
    )
}

scale_vars_to_baseline <- function(data, vars, baseline_year) {
  data_out <- data %>%
    mutate(
      across(vars, function(var) var / var[year == baseline_year])
    )

  return(data_out)
}
# ==============================================================================
# logistics of implementation
# ==============================================================================

print_log <- function(file, text = "upload to sql", filepath = here("log/log.txt")) {
  year <- str_extract(file, "\\d{4}")

  msg <- paste(text, year, "started.")
  
  print(msg)

  cat(
    msg,
    file = filepath,
    append = T
  )
}

write_log <- function(file, text = "upload to sql", filepath = here("log/log.txt")) {
  year <- str_extract(file, "\\d{4}")
  
  msg <- paste(text, year, "completed.")

  print(msg)

  cat(
    msg,
    file = filepath,
    append = T
  )
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
