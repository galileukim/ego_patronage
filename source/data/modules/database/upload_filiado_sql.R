# ---------------------------------------------------------------------------- #
message('setting up...')

debug <- TRUE

source(
    here::here("source/modules/database/requirements.R")
)

# ---------------------------------------------------------------------------- #
message("initiating upload")

levels <- c("mun", "state")
files <- sprintf("id/filiado_with_id_employee_%s.csv.gz", levels) %>%
    here_data()

filiado <- files %>%
    map(
        fread,
        integer64 = "character"
    ) %>%
    set_names(levels)

filiado <- filiado %>%
    map(
        ~ mutate(
            ., 
            across(
                starts_with('date'), ~format(., "%Y%m%d")
            )
        )
    )

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
