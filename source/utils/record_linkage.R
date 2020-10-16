group_nest_dt <- function(dt, ..., .key = "data") {
    stopifnot(is.data.table(dt))
    by <- substitute(list(...))

    dt <- dt[, list(list(.SD)), by = eval(by)]
    setnames(dt, old = "V1", new = .key)
    dt
}

unnest_dt <- function(dt, col, id) {
    stopifnot(is.data.table(dt))

    by <- substitute(id)
    col <- substitute(unlist(col, recursive = FALSE))
    dt[, eval(col), by = eval(by)]
}

filter_group_by_size <- function(data, ..., n = 1) {
    by <- substitute(list(...))

    data_filter <- data[
        ,
        if (.N == n) .SD,
        by = eval(by)
    ]

    return(data_filter)
}

nested_join <- function(nested_data) {
    data <- record_rais_filiados_list %>%
        modify(
            ~ mutate(
                .,
                kmer = substr(name, 1, 3),
                first_name = str_extract(name, "^[a-z]+")
            ) %>%
                setkey(first_name)
        )

    cat("nest and join rais and filiados data")
    record_rais_filiados_nested <- record_rais_filiados %>%
        modify(
            # ~ filter_group_by_size(., n = 1, name) %>%
            ~ filter_group_by_size(., name) %>%
                group_nest_dt(
                    .,
                    year, cod_ibge_6, kmer,
                    .key = "nested_data"
                ) %>%
                mutate(
                    nested_data = map(nested_data, ~ setkey(., name))
                )
        )
}

inner_merge <- function(x, y) {
    merge(x, y, all = FALSE)
}

reset_env <- function(init_env) {
    final_env <- ls(.GlobalEnv)

    rm(
        envir = .GlobalEnv,
        list = setdiff(final_env, init_env)
    )

    gc()
}

diagnose_duplicates <- function(data, group) {
    grouping <- substitute(group)

    diagnostic <- data[
        ,
        .(n = .N),
        by = eval(grouping)
    ][
        ,
        .(
            number_duplicated = sum(n > 1),
            density_duplicated = sum(n[n > 1]) / sum(n),
            total_records = sum(n)
        )
    ]

    return(diagnostic)
}

remove_duplicate_by_group <- function(data, group) {
    group <- substitute(group)

    data[
        ,
        n := .N,
        by = eval(group)
    ]

    data_deduplicated <- data[
        n == 1
    ] %>%
        select(-n)

    return(data_deduplicated)
}

clean_name <- function(name) {
    clean_name <- str_replace_all(name, "[^[:alpha:] ]", "") %>%
        str_to_lower() %>%
        iconv(to = "ASCII//TRANSLIT")

    return(clean_name)
}

match_fastLink <- function(x, y, threshold = 0.85) {
    match_out <- fastLink(
        dfA = x,
        dfB = y,
        varnames = c("first_name", "middle_name", "last_name"),
        stringdist.match = c("first_name", "middle_name", "last_name"),
        dedupe.matches = TRUE,
        n.cores = 1
    )

    matched_data <- getMatches(
        dfA = x,
        dfB = y,
        fl.out = match_out,
        threshold.match = threshold
    )

    matched_data_out <- matched_data %>%
        select(
            cpf,
            name,
            electoral_title
        )

    return(matched_data_out)
}

nrow_data <- function(data){
    nrows <- map_dbl(data, nrow)

    return(nrows)
}