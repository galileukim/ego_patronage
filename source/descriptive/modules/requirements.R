library(tidyverse)
library(dbplyr)
library(data.table)
library(here)
library(DBI)

data_path <- "data/clean/"
source_path <- "source/modules/database/"
rais_sql <- here(
  sprintf("data/clean/database/rais%s.sqlite3", 
  ifelse(isTRUE(debug), "_sample", "")
  )
)

rais_con <- DBI::dbConnect(RSQLite::SQLite(), rais_sql)

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

dbGetQuery <- purrr::partial(
  DBI::dbGetQuery,
  conn = rais_con
)

nrows <- if (isTRUE(debug)) 1e4 else Inf

# ==============================================================================
# data io
# ==============================================================================
list_files <- function(path, pattern) {
  files <- map2(
    path,
    pattern,
    list.files
  ) %>%
    flatten_chr()

  return(files)
}

# ==============================================================================
# rais wrangling
# ==============================================================================
is_municipal <- rlang::quo(nat_jur == 1031)
is_bureau <- rlang::quo(nat_jur == 1031 & occupation == 1)

year_to_char <- function(data) {
  data <- data %>%
    mutate_at(
      vars(year),
      as.character
    )

  return(data)
}

convert_to_date <- function(data){
  data %>%
  mutate(
        across(starts_with("date"), lubridate::ymd)
    )
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

create_municipal_dummy <- function(data){
  data %>%
    mutate(
      municipal = if_else(nat_jur == 1031, 1, 0)
    )
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


scale_vars_to_baseline <- function(data, vars, baseline_year) {
  data_out <- data %>%
    mutate(
      across(vars, function(var) var / var[year == baseline_year])
    )

  return(data_out)
}

# ==============================================================================
# ggplot aux functions
# ==============================================================================
# set theme
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
    GGally::ggcoef(
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
# gg_miss_var <- partial(
#   naniar::gg_miss_var,
#   show_pct = T
# )

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

gg_summary <- function(data, x, y, fun = 'mean', color = matte_indigo, smooth = T, ...){
  plot <- data %>% 
    ggplot(
      aes(
        !!sym(x),
        !!sym(y),
        color,
        ...
      )
    ) +
    stat_summary_bin(
      fun = fun,
      size = 1,
      geom = "point"
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
scale_colour_discrete <- function(palette = "OrRd", ...) scale_color_brewer(
  palette = palette, direction = -1, ...
)

scale_fill_discrete <- function(palette = "OrRd", ...) scale_fill_brewer(
  palette = palette, direction = -1, ...
)

matte_indigo <- "#375b7c"
matte_indigo2 <- "#4c6e8d"
tulip_red <- "#E64538"

update_geom_defaults(
  "bar",
  list(color = matte_indigo, fill = matte_indigo2)
)

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

# add mandate year vertical lines
plot_mandate_year <- function(years = seq(2005, 2013, 4)){
  geom_vline(
        xintercept = 
        as.numeric(as.Date(sprintf("%s-01-01", years)))
    )
}
