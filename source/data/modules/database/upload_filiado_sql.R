# ==============================================================================
# upload party membership data to sql database
# notes: only contains memebership data for those employed
# ==============================================================================
message("setting up...")

levels <- c("mun", "state")

source(
    here::here("source/data/modules/database/globals.R")
)

source(
    here::here("source/data/modules/database/requirements.R")
)

# ---------------------------------------------------------------------------- #
message("initiating upload raw filiado")

filiado <- fread(
    here("data/raw/tse/filiado.csv.gz")
)

# ---------------------------------------------------------------------------- #
message("initiating upload joined filiado")

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
        date_cancel DATE,
        year_start INTEGER,
        year_end INTEGER,
        year_cancel INTEGER,
        year_termination INTEGER
    );
    ",
    levels
)

create_table %>%
    walk(dbExecute)

files <- sprintf("filiado_with_id_employee_%s.csv.gz", levels) %>%
    here_data(type = "clean", dir = "id", file = .)

filiado <- files %>%
    map(
        fread
    ) %>%
    set_names(levels)

# add primary key
filiado <- filiado %>%
    map(
        ~ distinct(
            .,
            id_employee, date_start, date_end, party,
            .keep_all = TRUE
        ) %>%
            mutate(
                .,
                across(
                    starts_with("date"),
                    list(year = ~str_extract(., pattern = "\\d{4}")),
                    .names = "{fn}_{col}"
                ),
                year_termination = pmax(
                    year_date_end, year_date_cancel,
                    na.rm = T
                ) %>%
                    replace_na(2019),
                filiado_id = row_number()
            ) %>%
            rename_with(
                ~ stringr::str_replace(., "year_date", "year")
            )
    )

# fix date columns
filiado <- filiado %>%
    map(
        ~ mutate(
            .,
            across(
                starts_with("date"), ~ str_remove_all(., "-")
            ),
            across(
                starts_with("year"), as.integer
            )
        )
    )

# sample out
if (isTRUE(debug)) {
    filiado <- filiado %>%
        map(~ filter(., cod_ibge_6 == 110001))
}

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