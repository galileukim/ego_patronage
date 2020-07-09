# ==============================================================================
# fix spaece data: annual exams for the state of ceara
# ==============================================================================
spaece <- read_data(
    "raw",
    "spaece",
    "spaece.csv"
)

spaece <- spaece %>%
    select(
        -mun_name
    ) %>%
    rename(
        grade_level = grade
    )

spaece %>%
    write_data(
        "spaece",
        "spaece.rds"
    )