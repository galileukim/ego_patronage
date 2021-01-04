# aux funs for matching

blocked_fastLink <- function(year, state, df_rais, df_filiados) {
    t <- year
    s <- state

    rais <- df_rais %>%
        filter(
            year == t,
            state == s
        )

    filiados <- df_filiados %>%
        filter(
            (year_start < t) & (year_cancel > t) &
                state == s
        )

    rais_filiados_link <- fastLink(
        dfA = rais,
        dfB = filiados,
        varnames = c("first_name", "last_name", "middle_name"),
        stringdist.match = c("first_name", "last_name", "middle_name"),
        n.cores = 1
    )

    return(rais_filiados_link)
}

run_diagnostics_fastLink <- function(fastLink, rais, filiados) {
    matched_fastLink <- getMatches(
        dfA = rais,
        dfB = filiados,
        fl.out = fastLink,
        combine.dfs = F
    )

    # match rate by first name
    first_name_matches <- matched_fastLink %>%
        reduce(
            inner_join,
            by = c("first_name", "last_name")
        )

    exact_matches <-

    if (number_of_first_name_matches <= 40) {
        stop("match rate for first names less than 80 percent.")
    } else{
        print(sprintf("match rate is %s percent.",
        number_of_first_name_matches/50)
        )
    }

    # how many duplicated id_employees are there for each electoral title?
    proportion_duplicated_rais_filiados <- setDT(matched_fastLink)[
        , count := uniqueN(elec_title),
        by = id_employee
    ] %>%
        summarise(
            proportion_duplicated = mean(count > 1)
        ) %>%
        pull()

    if (proportion_duplicated_rais_filiados > 20) {
        stop("duplication rate is higher than 20 percent.")
    } else{
        print(sprintf("duplication rate is %s"),
        proportion_duplicated_rais_filiados)
    }
}