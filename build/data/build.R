# ==============================================================================
# set-up
# ==============================================================================
pacman::p_load(
  "tidyverse",
  "purrr",
  "data.table",
  "R.utils",
  "here",
  "parallel",
  "magrittr"
)

set.seed(1789)

source(
  here("build", "data", "functions.R")
)

# ==============================================================================
# build data repos
# ==============================================================================
repos <- c(
  "censo_br",
  "censo_escolar",
  "finbra",
  "rais",
  "saeb",
  "tse",
  "spaece"
)

walk(
  repos,
  ~ run_module(.)
)