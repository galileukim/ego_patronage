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
  here("source", "data", "functions.R")
)

read_data <- partial(
  read_data,
  type = "raw"
)