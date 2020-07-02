# functions ---------------------------------------------------------------
`%<>%` <- magrittr::`%<>%`
ivreg <- AER::ivreg
between <- data.table::between

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

run_module <- function(module) {
  print(
    paste0("running module ", module, "...")
  )

  build_repo(module)

  source_file <- paste0("datagen_", module, ".R")
  path <- here("build", "data", "modules", source_file)

  init_env <- ls(.GlobalEnv)
  source(path)
  reset_env(init_env)

  print(
    paste("module", module, "complete!")
  )
}

fread <- partial(
  data.table::fread,
  nThread = parallel::detectCores(),
  integer64 = c("character")
)

list.files <- partial(
  base::list.files,
  full.names = T
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

import_data <- function(path, pattern, names = F) {
  files <- list_files(
    path,
    pattern
  )

  if (names == F) {
    names <- basename(files) %>%
      str_remove_all(
        "\\.csv|\\.gz"
      )
  }

  data <- map(
    files,
    fread
  )

  names(data) <- names

  return(data)
}

assign_data <- function(data, rm = T) {
  walk2(
    names(data),
    data,
    assign,
    envir = globalenv()
  )
}

append_db <- function(path, pattern, conn, names = F) {
  data <- import(
    files,
    names
  )

  pwalk(
    list(
      name = names,
      value = data
    ),
    RSQLite::dbWriteTable,
    conn = con,
    overwrite = T
  )
}

fwrite_gz <- function(data, filename, overwrite = T) {
  data.table::fwrite(
    data,
    filename
  )

  R.utils::gzip(
    filename,
    overwrite = overwrite
  )
}

file_here <- function(paper = NULL, folder = NULL, file = NULL) {
  if (is.null(file)) {
    stop("no file name.")
  }

  path <- here(paste0("papers/paper_", paper), folder, file)
  return(path)
}

object_size <- function(object) {
  size <- object.size(object)

  print(size, units = "MB")
}

# data manipulation -------------------------------------------------------
# sample groups
sample_group <- function(data, n, ...) {
  grouping_vars <- enquos(...)

  data %<>%
    nest(
      cols = -c(!!!grouping_vars)
    ) %>%
    sample_n(n) %>%
    unnest(
      cols = -c(!!!grouping_vars)
    )

  return(data)
}

round_integer <- function(number, digits) {
  number %>%
    as.integer() %>%
    round(digits)
}

calc_turnover <- function(data, group_vars){
  years <- data %>% 
    distinct(year) %>% 
    pull
  
  data %<>%
    group_by_at(
      vars(
        group_vars
      )
    ) %>% 
    summarise_at(
      vars(starts_with("turnover_"), n),
      sum,
      na.rm = T
    ) %>% 
    ungroup()
  
  data %<>% 
    mutate(
      implicit = 0
    ) %>% 
    complete(
      year = years,
      fill = list(implicit = 1)
    ) %>% 
    group_by_at(
      vars(
        group_vars,
        -year
      )
    ) %>% 
    mutate(
      n_lag = dplyr::lag(n, order_by = year),
      percent_exit = turnover_exit/n_lag,
      percent_transfer = turnover_transfer/n_lag,
      percent_extinct = turnover_extinct/n_lag,
      percent_entry = turnover_entry/n,
      turnover_index = (turnover_exit + turnover_transfer + turnover_entry)/(n + n_lag)
    ) %>% 
    ungroup() %>% 
    filter(
      implicit != 1
    ) %>% 
    select(
      -implicit
    )
  
  return(data)
}

# tidy and ggcoef
tidyfit <- function(fit, vars = ".") {
  tidy(fit) %>%
    filter(
      !str_detect(term, "Intercept|as.factor|Observation.Residual"),
      str_detect(term, vars)
    ) %>%
    mutate(
      conf.low = estimate - 1.96 * std.error,
      conf.high = estimate + 1.96 * std.error
    )
}

# table of results
mstar <- function(..., font.size = "small", float = F, no.space = T, keep.stat = c("n", "rsq")) {
  stargazer::stargazer(
    ...,
    font.size = font.size,
    float = float,
    no.space = no.space,
    keep.stat = keep.stat
  )
}

# summarize props with c.i.
summarise_prop <- function(data, var) {
  summarise(
    .data = data,
    prop = mean(get(var), na.rm = T),
    n = n(),
    se = sqrt(prop * (1 - prop) / n),
    upper = prop + 1.96 * se,
    lower = prop - 1.96 * se
  )
}

# tailored summarise
summarise_stats <- function(data, ...) {
  vars <- enquos(...)

  data_count <- data %>%
    count()

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

count_freq <- function(data, ...) {
  vars <- enquos(...)

  data <- data %>%
    group_by(!!!vars) %>%
    summarise(n = n()) %>%
    mutate(freq = n / sum(n))

  return(data)
}

filter_if_n <- function(data, predicate) {
  predicate <- enquo(predicate)

  data %<>%
    mutate(n = n()) %>%
    filter(!!predicate) %>%
    ungroup() %>%
    select(-n)

  return(data)
}

# standardize
standardize <- function(data) {
  out <- data %>%
    mutate_if(
      is.double,
      scale
    )

  return(out)
}

# create first and last year
bind_electoral_term <- function(data) {
  out <- data %>%
    mutate(
      first_term = if_else(
        year %% 4 == 1,
        1, 0
      )
    )
}

group_split <- function(data, ...) {
  vars <- enquos(...)

  data <- data %>%
    group_by(!!!vars)

  group_names <- group_keys(data) %>%
    pull()

  data %>%
    dplyr::group_split() %>%
    set_names(
      group_names
    )
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