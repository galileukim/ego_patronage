# functions ---------------------------------------------------------------
`%<>%` <- magrittr::`%<>%`
ivreg <- AER::ivreg
between <- data.table::between

list.files <- purrr::partial(
  base::list.files,
  full.names = T
)

# io ----------------------------------------------------------------------
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

  if (str_detect(file, ".rds$")) {
    write_rds(object, file_path, compress = compress)
  } else {
    fwrite(object, file_path, compress = "gz")
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

# data manipulation -------------------------------------------------------
# check if vector of ids is unique
check_if_id_unique <- function(data, .group_vars) {
  data_count <- data %>%
    group_by(
      across(
        all_of({{.group_vars}})
      )
    ) %>%
    summarise(
      n = n(),
      .groups = "drop"
    )

  if(max(data_count$n) > 1) {
    print("id_vars are not unique. printing duplicates...")
    duplicated_groups <- data_count %>%
      filter(n > 1) %>%
      arrange(desc(n))

    return(duplicated_groups)
  }else {
    print("id_vars are unique.")
  }
}

# groups and summarises data
create_teacher_turnover_index <- function(data, .group_vars, .vars, complete_years){
  turnover_data <- data %>%
    grouped_sum(.group_vars, .vars) %>%
    complete_year_by_group(.group_vars, complete_years) %>%
    calc_turnover_by_group(.group_vars) %>%
    filter(
      completed != 1 # remove completed entries
    )

    return(turnover_data)
}

grouped_sum <- function(data, .group_vars, .vars){
  data %>%
    group_by_at(
      all_of(
       .group_vars
      )
    ) %>% 
    summarise(
      across(all_of({{.vars}}), sum, na.rm = T),
      .groups = "drop"
    )
}

complete_year_by_group <- function(data, .group_vars, complete_years){
  .group_vars_minus_year <- stringr::str_subset(.group_vars, "year", negate = T)
  
  complete_data <- data %>%
    mutate(
      completed = 0
    ) %>%
    select(
      all_of(
        c(.group_vars, "year")
      )
    ) %>%
    group_by_at(
      all_of(.group_vars_minus_year)
    ) %>%
    complete(
      year = complete_years,
      fill = list(completed = 1)
    ) %>%
    ungroup()

    return(complete_data)
}

calc_turnover_by_group <- function(data, .group_vars) {
   .group_vars_minus_year <- str_subset(.group_vars, "year", negate = T)

  turnover_data  <- data %>%
    group_by_at(
      vars(
        all_of(.group_vars_minus_year)
      )
    ) %>% 
    mutate(
      n_lag = dplyr::lag(n, order_by = year)
    ) %>%
    ungroup() %>%
    mutate(
      total_turnover = turnover_exit + turnover_transfer + turnover_entry,
      total_n = n + n_lag,
      turnover_index = ratio(total_turnover, total_n)
    )

  return(turnover_data)
}

ratio <- function(numerator, denominator){
  ratio <- numerator/denominator
  
  return(ratio)
}

summarise_stats <- function(data, ...){
  vars <- enquos(...)
  
  data_count <- data %>% 
    count
  
  data %<>% 
    summarise_at(
      .vars = vars(!!!vars),
      .funs = list(
        mean = mean,
        sum = sum, 
        med = median, 
        sd = sd
      ),
      na.rm = T
    ) %>% 
    ungroup()
  
  data %<>%
    left_join(
      data_count
    )
  
  return(data)
}

add_election_year <- function(data) {
  data <- data %>%
    mutate(
      election_year = case_when(
        between(year, 1985, 1988) ~ 1984,
        between(year, 1989, 1992) ~ 1988,
        between(year, 1993, 1996) ~ 1992,
        between(year, 1997, 2000) ~ 1996,
        between(year, 2001, 2004) ~ 2000,
        between(year, 2005, 2008) ~ 2004,
        between(year, 2009, 2012) ~ 2008,
        between(year, 2013, 2016) ~ 2012
      )
    )

  return(data)
}