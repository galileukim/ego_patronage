source(
    here::here("source/modules/database/requirements.R")
)

rais_con <- dbConnect(
    RSQLite::SQLite(),
    rais_sql
)
