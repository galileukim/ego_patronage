# ---------------------------------------------------------------------------- #
message('setting up...')

debug <- TRUE

source(
    here::here("source/modules/database/requirements.R")
)
# ---------------------------------------------------------------------------- #
message("initiating upload")

id_employee_rais <- dbGetQuery(
    rais_con,
    "SELECT DISTINCT id_employee as id_employee FROM rais"
)

levels <- c("mun", "state")
files <- sprintf("id/filiado_with_id_employee_%s.csv.gz", levels) %>%
    here_data()

filiado <- files %>%
    map(
        fread,
        integer64 = "character"
    ) %>%
    set_names(levels)

pwalk(
    list(
        name = sprintf("filiado_%s", levels),
        value = filiado
    ),
    dbWriteTable,
    conn = rais_con,
    overwrite = TRUE
)

message("uploading filiados data complete!")