# ==============================================================================
# input: additional tables containing demographic and political characteristics
# output: 1) demographic census. 2) electoral data. 3) budgetary data
# ==============================================================================
debug <- TRUE

source(
    here::here("source/data/modules/database/requirements.R")
)

# ---------------------------------------------------------------------------- #
censo <- read_data(
    "raw", "censo", "censo_mun_2010.csv"
)

electoral_data <- read_data(
    "raw", "tse", "election_chamber.csv"
)

finbra <- read_data(
    "raw", "finbra", "despesa_mun.csv"
)

tables_to_upload <- list(
    censo,
    electoral_data,
    finbra
)

table_names <- c(
    "censo_2010",
    "election",
    "budget"
)

pwalk(
    list(
    name = table_names,
    value = tables_to_upload
    ),
    dbWriteTable,
    conn = rais_con,
    overwrite = TRUE
)