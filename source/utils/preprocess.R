extract_unique_records <- function(data, ...){
    data_unique <- data %>%
        distinct(
            id_employee,
            name
        ) %>%
        create_split_name()
    
    return(data_unique)
}

clean_filiados <- function(data, ...) {
    clean_data <- data %>%
        select(
            cod_ibge_6,
            electoral_title,
            name = member_name,
            party,
            date_start,
            date_end,
            date_cancel,
            ...
        ) %>%
        mutate_all(
            as.character
        ) %>%
        create_split_name() %>%
        extract_year_from_dates() %>%
        mutate(
            state = str_sub(cod_ibge_6, 1, 2)
        )

    return(clean_data)
}

clean_rais <- function(data) {
    clean_data <- data %>%
        select(
            cod_ibge_6 = municipio,
            year,
            id_employee,
            name = nome,
            cpf
        ) %>%
        distinct() %>%
        mutate_all(
            as.character
        ) %>%
        create_split_name() %>%
        mutate(
            state = str_sub(cod_ibge_6, 1, 2)
        )

    return(clean_data)
}

create_split_name <- function(data) {
    data_with_split_names <- data %>%
        mutate(
            name = str_to_lower(name),
            split_names = str_match(
                name, "(^[a-z]+)\\s([a-z\\.\\s]+)\\s([a-z]+$)"
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
                year_date_end, year_date_cancel,
                na.rm = T
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

read_rais <- function(year, sample_size = Inf, path_to_rais = "/home/BRDATA/RAIS/") {
    rais <- read_dta(
        paste0(
            path_to_rais,
            sprintf("RAIS%s.dta", year)
        ),
        n_max = sample_size
    )

    return(rais)
}

arrange_by_name <- function(data, var, kmer = 3){
    ordered_data <- data %>%
        mutate(
            ordering = str_sub({{var}}, 1, kmer)
        ) %>%
        arrange(
            ordering
        ) %>%
        select(-ordering)

    return(ordered_data)
}

clean_names <- function(data, var){
    clean_data <- data %>%
        mutate(
            {{var}} := str_replace_all({{var}}, "[^[:alpha:] ]", "")
        )

    return(clean_data)
}