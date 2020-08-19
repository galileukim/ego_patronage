
read_7z <- function(file_path, year, select = NULL, dest_dir = tempdir()) {
    # extracts file from id folder
    # into a dest_folder, silently
    # returns data, cleans up the temp file
    dest_dir_temp <- sprintf(
        "%s/%s", dest_dir, "temp"
    )

    extraction_command <- sprintf(
        "7za e -o%s %s", dest_dir_temp, file_path
    )

    system(extraction_command)

    extracted_file_path <- list.files(
        dest_dir_temp,
        full.names = T
    )

    data <- map_dfr(
        extracted_file_path,
        ~ fread(
            .,
            colClasses = "character",
            select = select,
            encoding = "Latin-1"
        )
    )

    unlink(dest_dir_temp, recursive = T)

    return(data)
}

extract_id_file_names <- function(year, debug) {
    print(
        sprintf("start producing hash for year %s", year)
    )

    id_file_path <- list.files(
        sprintf("%s/%s/", id_path, year),
        pattern = "7z$",
        full.names = T
    )

    if (isTRUE(debug)) {
          id_file_path <- sample(id_file_path, 1)
      } else {
          id_file_path <- id_file_path
      }

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

write_out_hash <- function(hash, filename) {
    tibble(
        cpf = keys(hash),
        name = values(hash, USE.NAMES = F)
    ) %>%
        as.data.table() %>%
        fwrite(
            sprintf(here("data/clean/id/rais_hash/%s"), filename)
        )
}