# fix year
debug <- TRUE
source(
    here::here("source/data/modules/database/requirements.R")
)

if (dbExistsTable(rais_con, "rais_corrected")) {
    dbRemoveTable(rais_con, "rais_corrected")
}

res <- dbSendQuery(rais_con, "SELECT * FROM rais")

while (!dbHasCompleted(res)) {
    rais <- dbFetch(res, n = 1e4)
    rais_corrected <- rais %>% mutate(across(year, as.integer))
    print(dbHasCompleted(res))
    print(nrow(rais_corrected))

    # dbWriteTable(
    #     conn = rais_con,
    #     name = "rais_corrected",
    #     value = rais_corrected,
    #     row.names = F,
    #     append = TRUE
    # )
}

dbClearResult(res)
dbDisconnect(rais_con)
