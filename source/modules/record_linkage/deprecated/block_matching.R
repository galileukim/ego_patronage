# subset by year and state
print("perform block merge (state-year) with fastLink")

# years <- 2003:2019
years <- 2006

states <- filiados_clean %>%
    distinct(state) %>%
    pull() %>%
    sort() %>%
    sample(3)

for (i in seq_along(years)) {
    t <- years[i]

    rais_deduped <- rais_merge %>%
        select(# extract unique id's by employee.
            cod_ibge_6,
            year,
            id_employee,
            cpf,
            contains("name")
        ) %>%
        distinct()

    rais_filiados_link <- map(
        states,
        ~ blocked_fastLink(
            t,
            .,
            rais_merge,
            filiados_merge
        )
    )

    result_diagnostics <- rais_filiados_link %>%
        map(
            ~ run_diagnostics_fastLink(
                .,
                rais_merge,
                filiados_merge
            )
        )

    matched_rais_filiados <- rais_filiados_link %>%
        map(
            ~ getMatches(
                dfA = rais_merge,
                dfB = filiados_merge,
                fl.out = .,
                combine.dfs = T
            )
        )

    # extract bureaucrat id's for partisan members
    filiados_with_employee_id <- matched_rais_filiados %>%
        select(
            cod_ibge_6,
            year,
            id_employee,
            elec_title,
            party,
            starts_with("date"),
        )

    filiados_with_employee_id %>%
        fwrite(
            sprintf(here("data/clean/filiados_with_id_%s.csv"), years[i])
        )
}