# ==============================================================================
# input: sql database of party affiliation
# output: compare transition matrices for partisans and non-partisans
# into the bureaucracy
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

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
    rename(
        year_entry = year,
        cbo_02_entry = cbo_02,
        wage_entry = wage
    ) %>%
    collect() %>%
    filter(
        !is.na(cbo_02_entry)
    )

# last private sector job
private_last_job <- dbGetQuery(
    "
    SELECT 
    rais.id_employee,
    rais.year,
    rais.cod_ibge_6,
    rais.cbo_02,
    rais.wage
    FROM rais
    INNER JOIN rais_bureaucrat_entry
    ON rais.id_employee = rais_bureaucrat_entry.id_employee
    AND rais.year <= rais_bureaucrat_entry.year
    WHERE nat_jur != 1031 AND cbo_02 IS NOT NULL
    GROUP BY rais.id_employee
    HAVING rais.year = MAX(rais.year)
    "
)

private_last_job <- private_last_job %>%
    group_by(id_employee) %>%
    ungroup()

# join at the individual level last private sector job and bureaucracy job
transition_private_bureaucracy <- private_last_job %>%
    left_join(
        bureaucracy_entry_job,
        by = c("cod_ibge_6", "id_employee")
    ) %>%
    mutate(
        across(starts_with("cbo_02"), ~str_sub(., 1, 1))
    )

transition_occupation <- transition_private_bureaucracy %>%
    count(
        cbo_02,
        cbo_02_entry
    )

transition_occupation <- transition_occupation %>%
    complete(
        cbo_02,
        cbo_02_entry,
        fill = list(n = 0)
    )

# plot out
plot_transition_occupation <- transition_occupation %>%
    ggplot() +
    geom_tile(
        aes(
            x = cbo_02,
            y = cbo_02_entry,
            fill = round(n/1000, 2)
        )
    ) +
    scale_fill_continuous(
        name = "Total",
        type = "viridis"
    ) +
    labs(
        x = "private sector occupation",
        y = "bureaucracy occupation",
        caption = "Total (thousands)"
    )

message("export plot")
path_to_figs <- ifelse(
    isTRUE(debug), 
    "paper/figures/turnover/sample/", 
    "paper/figures/turnover/"
)

save_plot(
    plot_transition_occupation,
    filename = here(path_to_figs, "plot_transition_occupation.pdf")
)
