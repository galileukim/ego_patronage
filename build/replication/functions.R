# ==============================================================================
# workflow functions
# ==============================================================================
`%<>%` <- magrittr::`%<>%`

between <- data.table::between

build_repo <- function(module){
  repo <- here("data", "clean", module)

  if(!dir.exists(repo)){
    dir.create(repo)
  }else{
    unlink(repo, recursive = T)
    dir.create(repo)
  }
}

list_data <- function(dir){
  path <- here("data", "clean", dir)
  files <- list.files(path)

  return(files)
}

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
  
  print_obj_size(data)

  return(data)
}

print_obj_size <- function(obj) {
  obj_size <- object.size(obj)
  print(obj_size, units = "MB")
}

ls_data <- function(env = .GlobalEnv) {
  obj <- ls(env)

  is_data <- map_lgl(
    obj,
    ~is.data.frame(get(.))
  )

  data_obj <- obj[is_data]

  return(data_obj)
}

read_model <- function(file){
  fit <- read_rds(
    here("replication", "models", file)
  )

  print_obj_size(fit)

  return(fit)
}

write_model <- function(model, file) {
  print_obj_size(model)
  
  write_rds(
    model,
    here("replication", "models", file)
  )
}

load_p <- function(packages){
  walk(
    packages,
    ~library(., character.only = T)
  )
}

detach_p <- function(packages){
  packages %>%
    purrr::walk(
      ~detach(
      paste0("package:", .),
      character.only = T,
      unload = T
    )
  )
}

run_module <- function(domain, module) {
  print(
    paste0("running module ", module, "...")
  )

  source_setup(domain)

  if(domain == "data"){
    build_repo(module)
  }
  
  module_script <- paste0(module, ".R")

  path <- here("build", domain, "modules", module_script)

  init_env <- ls(.GlobalEnv)
  source(path)
  reset_env(init_env)

  print(
    paste("module", module, "complete!")
  )
}

reset_env <- function(init_env, packages){
  final_env <- ls(.GlobalEnv)

  rm(
    envir = .GlobalEnv,
    list = setdiff(final_env, init_env)
  )

  gc()
}

save_fig <- function(pl, file, width = 5, height = 3, ...){
  ggplot2::ggsave(
    filename = here("replication", "figs", file),
    pl,
    width = width,
    height = height,
    ...
  )
}

# data io -----------------------------------------------------------------
fread <- partial(
  data.table::fread,
  nThread = parallel::detectCores(),
  integer64 = c("character")
)

list_files <- function(path, pattern){
  files <- map2(
    path,
    pattern,
    list.files
  ) %>% 
    flatten_chr
  
  return(files)
}

# import_data <- function(path, pattern, names = F){
#   files <- list_files(
#     path,
#     pattern
#   )
  
#   if(names == F){
#     names <- basename(files) %>% 
#       str_remove_all(
#         "\\.csv|\\.gz"
#       )
#   }
  
#   data <- map(
#     files,
#     fread
#   )
  
#   names(data) <- names
  
#   return(data)
# }

# assign_data <- function(data, rm = T){
#   walk2(
#     names(data),
#     data,
#     assign,
#     envir = globalenv()
#   )
# }

# append_db <- function(path, pattern, conn, names = F){
#   data <- import(
#     files,
#     names
#   )
  
#   pwalk(
#     list(
#       name = names,
#       value = data
#     ),
#     RSQLite::dbWriteTable,
#     conn = con,
#     overwrite = T
#   )
# }

# ==============================================================================
# data cleaning
# ==============================================================================
# sample groups
sample_group <- function(data, n, ...){
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
    as.integer %>% 
    round(digits)
}

select_starts <- function(data, pattern){
  out <- data %>%
    select(
      starts_with(pattern)
    )

  return(out)
}

# calc_turnover <- function(data, group_vars){
#   years <- data %>% 
#     distinct(year) %>% 
#     pull
  
#   data %<>%
#     group_by_at(
#       vars(
#         group_vars
#       )
#     ) %>% 
#     summarise_at(
#       vars(starts_with("turnover_"), n),
#       sum,
#       na.rm = T
#     ) %>% 
#     ungroup()
  
#   data %<>% 
#     mutate(
#       implicit = 0
#     ) %>% 
#     complete(
#       year = years,
#       fill = list(implicit = 1)
#     ) %>% 
#     group_by_at(
#       vars(
#         group_vars,
#         -year
#       )
#     ) %>% 
#     mutate(
#       n_lag = dplyr::lag(n, order_by = year),
#       percent_exit = turnover_exit/n_lag,
#       percent_transfer = turnover_transfer/n_lag,
#       percent_extinct = turnover_extinct/n_lag,
#       percent_entry = turnover_entry/n,
#       turnover_index = (turnover_exit + turnover_transfer + turnover_entry)/(n + n_lag)
#     ) %>% 
#     ungroup() %>% 
#     filter(
#       implicit != 1
#     ) %>% 
#     select(
#       -implicit
#     )
  
#   return(data)
# }

# tidy and ggcoef
tidyfit <- function(fit, vars = '.'){
  broom::tidy(fit) %>%
    filter(
      !str_detect(term, "Intercept|as.factor|Observation.Residual"),
      str_detect(term, vars)
    ) %>%
    mutate(
      conf.low = estimate - 1.96*std.error,
      conf.high = estimate + 1.96*std.error
    )
}

# table of results
mstar <- function(..., font.size = "small", float = F, no.space = T, keep.stat = c("n", "rsq")){
  stargazer::stargazer(
    ...,
    font.size = font.size, 
    float = float, 
    no.space = no.space, 
    keep.stat = keep.stat
  )
}

# summarize props with c.i.
summarise_prop <- function(data, var){
  summarise(
    .data = data,
    prop = mean(get(var), na.rm = T),
    n = n(),
    se = sqrt(prop*(1 - prop)/n),
    upper = prop + 1.96*se,
    lower = prop - 1.96*se
  )
}

# tailored summarise
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

# create group summary
group_summarise <- function(data, group_vars, summarise_vars, ...){
  out <- data %>%
    group_by(
      across({{group_vars}})
    ) %>%
    summarise(
      across({{summarise_vars}},
      ...
      ),
      .groups = 'drop'
    )

    return(out)
}

count_freq <- function(data, ...){
  vars <- enquos(...)
  
  data <- data %>% 
    group_by(!!! vars) %>% 
    summarise(n = n()) %>% 
    mutate(freq = n/sum(n))
  
  return(data)
}

filter_if_n <- function(data, predicate){
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

# visualization -----------------------------------------------------------
theme_clean <- theme(
  panel.background = element_blank(),
  # panel.border = element_blank(),
  panel.grid.major = element_blank(),
  panel.grid.minor = element_blank(),
  axis.line = element_line(colour = "grey35"),
  legend.position = "bottom",
  text = element_text(
    # family = "Inconsolata",
    color = "grey30",
    size = 10
  ),
  axis.text = element_text(color = "grey30", size = 8),
  axis.title = element_text(size = 10),
  plot.margin = unit(rep(0.25, 4), "cm")
)

theme_set(
  theme_minimal() +
    theme_clean
)

tidycoef <- function(fit, vars = ".", ...){
  tidyfit(fit, vars) %>%
    ggcoef(
      mapping = aes_string(
        y = "term", 
        x = "estimate",
        ...
      ),
      vline_color = "grey85",
      vline_linetype = "dotted",
      color = "steelblue3",
      sort = "ascending",
      errorbar_color = "steelblue3"
    ) +
    xlab("") +
    theme_clean
}

# add mandate year vertical lines
mandate_year <- function(years = seq(2005, 2013, 4)){
  geom_vline(
    xintercept = years,
    linetype = "dotted",
    color = "grey65"
  )
}

# summarize by mun
summarise_fun <- function(data, var){
  data %>%
    group_by(
      region,
      state,
      mun_id,
      year
    ) %>%
    mutate(
      private_var = ifelse(
        public == 0,
        get(var),
        NA
      )
    ) %>%
    summarise(
      public = mean(
        ifelse(
          public == 1,
          get(var),
          NA
        ),
        na.rm = T
      ),
      private = mean(
        private_var,
        na.rm = T
      )
    )
}

# ==============================================================================
# estimation aux. functions
# ==============================================================================
ivreg <- AER::ivreg

formulate_models <- function(response, predictor, fe, controls) {
  formulae <- c(
    reformulate(
      c(predictor, fe), response
    ),
    reformulate(
      c(predictor, controls), response
    )
  )

  return(formulae)
}

# ols
fit_felm <- function(
  repo,
  dv,
  predictor = c("coalition_share"),
  control = c("mayor_age", "as.factor(mayor_party)", "mayor_coalition_size", "mayor_campaign", "median_wage", "rais_mun_size", "rais_permanent", "mean_edu", "effective_parties"),
  cluster = c("state + year"),
  data
){
  fit <- data %>%
    felm(
      formula = formula(
        paste(
          dv, "~", predictor, "+", str_c(control, collapse = "+"),
          "|", cluster, "| 0"
        )
      ),
      data = .
    )
  
  return(fit)
}

# logit
logit <- function(f, data){
  fit <- glm(
    formula = f,
    data = data,
    family = 'binomial'
  )
}

# election years
add_election <- function(data){
  data %>% 
    mutate(
      election_year = case_when(
        between(year, 2001, 2004) ~ 2000,
        between(year, 2005, 2008) ~ 2004,
        between(year, 2009, 2012) ~ 2008,
        between(year, 2013, 2016) ~ 2012
      )
    )
}

# covariates
join_covariate <- function(data){
  data %>% 
    left_join(
      read_rds(
        here("data/clean/finbra/finbra.rds")
      ),
      by = c("cod_ibge_6", "year")
    ) %>% 
    left_join(
      read_rds(
        here("data/clean/censo_br/censo_2000.rds")
      ),
      by = c("cod_ibge_6")
    ) %>% 
    add_election() %>% 
    left_join(
      read_rds(
        here("data/clean/tse/election.rds")
      ),
      by = c("cod_ibge_6", "election_year")
    )
}

## ggplot aux
gg_point_smooth <- function(data, mapping = aes(),...){
  ggplot(
    data = data,
    mapping
  ) +
    geom_point(
      alpha = 0.25
    ) +
    geom_smooth(
      method = "lm",
      col = "coral3"
    ) 
}

gg_point_line <- function(data, mapping = aes(), ...) {
  ggplot(
    data = data,
    mapping
  ) +
    geom_point(
      ...
    ) +
    geom_line(
      ...
    )
}

geom_errorbar_tidy <- geom_errorbarh(
    aes(
      xmin = conf.low,
      xmax = conf.high
    ),
    height = 0,
    linetype = "solid",
    position = ggstance::position_dodgev(height=0.3)
  )

# missingness
gg_miss_var <- partial(
  naniar::gg_miss_var,
  show_pct = T
)

# hex
gg_hex <- function(data, mapping = aes(), n_bin = 30, ...){
  ggplot(
    data = data,
    mapping
  ) +
    geom_hex(
      bins = n_bin,
      ...
    ) +
    scale_fill_distiller(
      palette = "RdYlBu",
      direction = -1
    )
}
  
# histogram
gg_histogram <- function(data, mapping = aes(), ...){
  ggplot(
    data = data,
    mapping
  ) +
    geom_histogram(
      ...,
      col = "#375b7c",
      fill = "steelblue3",
      aes(y = stat(width*density))
    )
}

gg_point <- function(data, mapping = aes(), ...){
  ggplot(
    data = data,
    mapping
  ) +
    geom_point(...)
}

gg_summary <- function(data, x, y, fun = 'mean', size = 2, geom = 'point', color = matte_indigo, smooth = T, ...){
  plot <- data %>% 
    ggplot(
      aes(
        !!enquo(x),
        !!enquo(y)
      )
    ) +
    stat_summary_bin(
      fun = fun,
      size = size,
      geom = geom,
      color = color,
      ...
    )
  
  if(smooth == T){
    plot <- plot +
      geom_smooth(
        method = 'gam',
        formula = y ~ splines::bs(x, 3)
      ) 
  }
  
  return(plot)
}

# change default ggplot settings
scale_colour_discrete <- function(...) scale_color_brewer(palette = "OrRd", direction = -1, ...)
scale_fill_discrete <- function(...) scale_fill_brewer(palette = "OrRd", direction = -1, ...)

matte_indigo <- "#375b7c"
tulip_red <- "#E64538"

update_geom_defaults(
  "point",
  list(color = matte_indigo, size = 1.5)
)

update_geom_defaults(
  "vline",
  list(linetype = "dashed", color = "grey65")
)

update_geom_defaults(
  "line",
  list(color = matte_indigo, size = 1)
)

update_geom_defaults(
  "smooth",
  list(color = tulip_red)
)

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

# reset
# rm(
#   list = setdiff(ls(), c(lsf.str(),"theme_clean"))
# )