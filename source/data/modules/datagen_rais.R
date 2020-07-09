# rais --------------------------------------------------------------------
# municipal bureaucracy data
# output: municipal tables of teachers, school principals, and bureaucrats
rais_mun <- read_data(
    "raw", "rais", "rais_mun.csv.gz"
)

rais_mun <- rais_mun %>%
    add_election_year()

rais_mun <- rais_mun %>%
    select(
        cod_ibge_6,
        cbo_category,
        year,
        election_year,
        everything()
    )

rais_mun %>%
    write_data("rais", "rais_mun.rds")

# teacher and school principal data
rais_edu <- read_data(
    "raw", "rais", "rais_edu.csv.gz"
)

# create municipal level data
rais_edu_mun <- rais_edu %>%
    group_by(
        cod_ibge_6,
        rais_category,
        year
    ) %>%
    summarise(
        rais_adm = mean(rais_adm, na.rm = T),
        rais_size = n(),
        rais_higher_edu = mean(rais_edu >= 9, na.rm = T),
        rais_fired = mean(rais_fired, na.rm = T),
        rais_hired = mean(rais_hired, na.rm = T),
        rais_permanent = mean(rais_permanent, na.rm = T),
        rais_rehired = mean(rais_rehired, na.rm = T),
        rais_time = mean(rais_time, na.rm = T),
        rais_wage = median(rais_wage, na.rm = T)
    ) %>%
    ungroup()

rais_edu_mun <- rais_edu_mun %>%
    add_election_year()

rais_edu_mun <- rais_edu_mun %>%
    select(
        cod_ibge_6,
        year,
        election_year,
        rais_category,
        everything()
    )

rais_edu_mun %>%
    write_data(
        "rais", "rais_mun_edu.rds"
    )