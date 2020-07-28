# aux funs for matching
create_split_name <- function(data) {
    data_with_split_names <- data %>%
        mutate(
            name = str_to_lower(name),
            split_names = str_match(
                name, "(^[a-z]+)\\s([a-z\\s]+)\\s([a-z]+$)"
            ),
            first_name = split_names[, 2],
            middle_name = split_names[, 3],
            last_name = split_names[, 4]
        ) %>%
        select(-split_names)

    return(data_with_split_names)
}

extract_year_from_dates <- function(data) {
    data_with_years <- data %>%
        mutate(
            across(
                c(starts_with("date")),
                extract_year,
                .names = "year_{col}"
            ),
            year_termination = pmax(
                year_date_end, year_date_cancel, na.rm = T
            ) %>%
                replace_na(2019)
        ) %>%
        rename_with(
            ~ str_replace(., "year_date", "year"),
            starts_with("year_date")
        )

    return(data_with_years)
}

extract_year <- function(col) {
    year_col <- str_extract(col, "\\d{4}") %>%
        as.integer()

    return(year_col)
}

blocked_fastLink <- function(year, state, df_rais, df_filiados) {
    rais <- rais_filiados_no_cpf %>%
        filter(
            year == t,
            state == s
        )

    filiados <- filiados_clean_with_years %>%
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
        fl.out = rais_filiados_link,
        combine.dfs = F
    )

    # match rate by first name
    number_of_first_name_matches <- matched_fastLink %>%
        map(
            ~ head(., 50)
        ) %>%
        reduce(
            inner_join,
            by = "first_name"
        ) %>%
        nrow()

    if (number_of_first_name_matches <= 40) {
        print("match rate for first names less than 80 percent.")
        break
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
        print("duplication rate is higher than 20 percent.")
        break
    }
}