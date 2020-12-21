# ==============================================================================
# input: sql database of party affiliation
# output: compare transition matrices for partisans and non-partisans
# into the bureaucracy
# ==============================================================================
debug <- TRUE

source(
    here::here("source/descriptive/modules/requirements.R")
)

rais <- tbl(rais_con, "rais")
filiado <- tbl(rais_con, "filiado_mun")
bureaucracy_entry <- tbl(rais_con, "rais_bureaucrat_entry")

# ---------------------------------------------------------------------------- #
message("generate transition matrix")

# extract entry into bureaucracy
join_cols <- c("id_employee", "cod_ibge_6", "year")
outcome_cols <- c("cbo_02", "wage")
rais_selected <- rais %>%
    select(
        all_of(
            c(join_cols, outcome_cols)
        )
    )

# generate entry level job for bureaucracy
bureaucracy_entry_job <- rais_selected %>%
    inner_join(
        bureaucracy_entry,
        on = c("id_employee", "cod_ibge_6", "year")
    )

# prepare data for join
bureaucracy_entry_job <- bureaucracy_entry_job %>%
    mutate(
        year = as.character(as.integer(as.integer(year) - 1.0))
    ) %>%
    rename(
        cbo_02_lead = cbo_02,
        wage_lead = wage
    )

# last private sector job
private_last_job <- dbGetQuery(
    "
    SELECT * FROM rais
    LEFT JOIN rais_bureaucrat_entry
    ON rais.id_employee = rais_bureaucrat_entry.id_employee
    AND rais.year < rais_bureaucrat_entry.year
    "
)

rais_entry_job <- rais_selected %>% 
    left_join(
        bureaucracy_entry_job,
        on = c("id_employee", "cod_ibge_6", "year")
    )
