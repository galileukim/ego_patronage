# ---------------------------------------------------------------------------- #
message("setting up...")

debug <- TRUE
levels <- c("mun", "state")
source(
    here::here("source/modules/database/requirements.R")
)

# ---------------------------------------------------------------------------- #
message("initiating upload")

# initiate tables
walk(
    sprintf("filiado_%s", levels),
    dbRemoveTable,
    conn = rais_con
)

create_table <- sprintf(
    "
    CREATE TABLE filiado_%s(
        filiado_id INTEGER PRIMARY KEY,
        id_employee INTEGER,
        cod_ibge_6 TEXT,
        party TEXT,
        date_start DATE,
        date_end DATE,
        date_cancel DATE
    );
    ",
    levels
)

create_table %>%
    walk(
        dbExecute,
        conn = rais_con
    )

files <- sprintf("id/filiado_with_id_employee_%s.csv.gz", levels) %>%
    here_data()


filiado <- files %>%
    map(
        fread,
        integer64 = "character"
    ) %>%
    set_names(levels)

# add primary key
filiado <- filiado %>%
    map(
        ~ mutate(., filiado_id = row_number())
    )

# fix date columns
filiado <- filiado %>%
    map(
        ~ mutate(
            .,
            across(
                starts_with("date"), ~ format(., "%Y%m%d")
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
    append = TRUE
)

message("uploading filiados data complete!")
