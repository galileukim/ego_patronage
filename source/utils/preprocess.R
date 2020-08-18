extract_unique_records <- function(data, ...) {
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

read_7z <- function(file_path, year, select = NULL, dest_dir = tempdir()) {
    # extracts file from id folder
    # into a dest_folder, silently
    # returns data, cleans up the temp file
    dest_dir_temp <- sprintf(
        "%s/%s", dest_dir, "temp"
    )

    extraction_command <- sprintf(
        "7za e -aoa -o%s %s", dest_dir_temp, file_path
    )

    system(extraction_command)

    extracted_file_path <- list.files(
        dest_dir_temp,
        full.names = T
    )

    data <- fread(
        extracted_file_path,
        colClasses = "character",
        select = select,
        encoding = "Latin-1"
    )

    unlink(dest_dir_temp, recursive = T)

    return(data)
}

# update_hash <- function(hash, data) {
#     keys <- data[["cpf"]]
#     values <- data[["name"]]

#     index <- !has.key(keys, hash)
#     names(index) <- NULL

#     new_keys <- keys[index]
#     new_values <- values[index]

#     hash[new_keys] <- new_values

#     return(hash)
# }

extract_id_file_names <- function(year, debug){
    print(
        sprintf("start producing hash for year %s", year)
    )

    id_file_path <- list.files(
        sprintf("%s/%s/", id_path, year),
        pattern = "7z$",
        full.names = T
    )

    if (isTRUE(debug))
        id_file_path <- sample(id_file_path, 1) 
    else 
        id_file_path <- id_file_path

    return(id_file_path)
}

clean_name <- function(name) {
    clean_name <- str_replace_all(name, "[^[:alpha:] ]", "") %>%
        str_to_lower() %>%
        iconv(to = "ASCII//TRANSLIT")

    return(clean_name)
}

extract_unique_cpf <- function(data, key = "cpf") {
    unique_data <- data[!c("0", "99")] %>%
        unique(
            by = key
        )

    return(unique_data)
}

write_out_hash <- function(hash, filename){
     tibble(
        cpf = keys(hash),
        name = values(hash, USE.NAMES = F)
    ) %>%
        as.data.table() %>%
        fwrite(
            sprintf(here("data/clean/id/rais_hash/%s"), filename)
        )
}
