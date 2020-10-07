extract_unique_records <- function(data, ...) {
    data_unique <- data %>%
        distinct(
            id_employee,
            name
        ) %>%
        create_split_name()

    return(data_unique)
}

clean_name <- function(name) {
    clean_name <- str_replace_all(name, "[^[:alpha:] ]", "") %>%
        str_to_lower() %>%
        iconv(to = "ASCII//TRANSLIT")

    return(clean_name)
}

clean_filiados <- function(data, ...) {
    clean_data <- data %>%
        transmute(
            cod_ibge_6,
            electoral_title,
            name = clean_name(member_name)
            # party,
            # date_start,
            # date_end,
            # date_cancel,
            # ...
        ) %>%
        mutate_all(
            as.character
        )
    # create_split_name() %>%
    # extract_year_from_dates() %>%
    # mutate(
    #     state = str_sub(cod_ibge_6, 1, 2)
    # )

    return(clean_data)
}

dedupe_data <- function(data, vars) {
    unique_data <- unique(data, by = vars)

    return(unique_data)
}

create_split_name <- function(data, var = name) {
    data_with_split_names <- data %>%
        mutate(
            {{ var }} := clean_name({{ var }}),
            split_names = str_match(
                {{ var }}, "(^[a-z]+)\\s([a-z\\.\\s]+)\\s([a-z]+$)"
            ),
            first_name = split_names[, 2],
            middle_name = split_names[, 3],
            last_name = split_names[, 4]
        ) %>%
        select(-split_names)

    return(data_with_split_names)
}

extract_first_name <- function(string){
    first_name <- str_subset(string, "(^[a-z]+)")
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

arrange_by_name <- function(data, var, kmer = 3) {
    ordered_data <- data %>%
        mutate(
            ordering = str_sub({{ var }}, 1, kmer)
        ) %>%
        arrange(
            ordering
        ) %>%
        select(-ordering)

    return(ordered_data)
}

rm_dir_files <- function(dir){
    files <- list.files(dir, full.names = T)

    file.remove(files)
}