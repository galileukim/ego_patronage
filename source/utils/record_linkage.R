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
s
nested_join <- function(nested_data){
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
                    year, cod_ibge_6, kmer, .key = "nested_data"
                ) %>%
                mutate(
                    nested_data = map(nested_data, ~ setkey(., name))
                )
        )
}

inner_merge <- function(x, y){
    merge(x, y, all = FALSE)
}

reset_env <- function(init_env){
  final_env <- ls(.GlobalEnv)

  rm(
    envir = .GlobalEnv,
    list = setdiff(final_env, init_env)
  )

  gc()
}