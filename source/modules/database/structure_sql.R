# input: rais_sql database
# output: 
# 1) table of only filiados in rais
# 2) table of only political candidates in rais
# 3) incorporate additional municipal covariates
debug <- TRUE

source(
    here::here("source/modules/database/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")

filiado_id <- 
rais_2003 <- dbGetQuery(
    rais_con,
    "SELECT * FROM rais WHERE year = '2003' LIMIT 1000"
)
