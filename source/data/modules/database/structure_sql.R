# input: rais_sql database
# output: 
# 1) table of only filiados in rais
# 2) table of only political candidates in rais
# 3) incorporate additional municipal covariates
debug <- TRUE

source(
    here::here("source/data/modules/database/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")
id_employee_rais <- rais %>%
    distinct(id_employee)

filiado_rais <- filiado %>%
    inner_join(
        id_employee_rais,
        on = "id_employee"
    )

filiado_rais <- filiado_rais %>%
    collect()
    