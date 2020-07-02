# ==============================================================================
# set-up
# ==============================================================================
pacman::p_load(
  tidyverse,
  data.table,
  here
  # GGally,
  # lfe,
  # lme4,
  # plm,
  # broom,
  # extrafont,
  # rprojroot,
  # tmap,
  # gridExtra,
  # cowplot,
  # ggdag,
  # scales,
  # kableExtra,
  # RColorBrewer,
  # gghighlight,
  # parallel,
  # foreach,
  # gridExtra,
  # naniar,
  # knitr,
  # gghighlight,
  # egg,
  # WeightIt,
  # cobalt,
  # boot,
  # rdrobust,
  # magrittr,
  # sjPlot
)

knitr::opts_chunk$set(
  eval = T,
  cache = T,
  cache.lazy = F,
  message = F,
  warning = F,
  echo = F,
  fig.height = 3,
  fig.width = 4,
  fig.align = "center"
  # dev = "pdf"
)

# connect to database
con <- DBI::dbConnect(
  odbc::odbc(),
  driver = "PostgreSQL Unicode",
  database = "rais",
  UID = "gali",
  PWD = 'gali1789!',
  port = 5432
)

set.seed(1789)

source(
  here("build", "replication", "functions.R")
)

run_module <- partial(
  run_module,
  domain = "replication"
)

read_data <- partial(
  read_data,
  type = "clean"
)