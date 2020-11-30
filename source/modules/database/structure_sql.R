# input: rais_sql database
# output: create table of 
source(
    here::here("source/modules/database/requirements.R")
)

rais_con <- dbConnect(
    RSQLite::SQLite(),
    rais_sql
)

rais <- tbl(rais_con, "rais") 

rais_2003 <- dbGetQuery(
    rais_con,
    "SELECT * FROM rais WHERE year = '2003' LIMIT 1000"
)
