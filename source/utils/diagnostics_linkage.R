summarise_duplicates <- function(data) {
    # generates proportion of entries duplicated
    # and the density of duplicated entries
    # note it expects the var name to be n
    data_summary <- data[
        ,
        .(
            prop_duplicated = mean(n > 1),
            density_duplicated = sum(n > 1) / sum(n)
        )
    ]

    return(data_summary)
}

count_dt <- function(data, ...) {
    # generates the count of values
    var <- substitute(list(...))

    data_count <- data[
        ,
        .(n = .N),
        by = eval(var)
    ]

    return(data_count)
}

count_unique_by_group <- function(data, col, by) {
    col <- substitute(unlist(col, recursive = FALSE))
    by <- substitute(by)

    data_count_unique <- data[
        ,
        .(n = uniqueN(eval(col))),
        by = eval(by)
    ]

    return(data_count_unique)
}

generate_diagnostics_rais <- function(data) {
    # generate a diagnostics table from rais
    year <- unique(data[["year"]])

    data_sample <- data[sample(1:nrow(data), 1e6, replace = FALSE)]

    data_diagnostics_name <- data_sample %>%
        setkey(name) %>%
        count_dt(name) %>%
        summarise_duplicates()

    data_diagnostics_cpf <- data_sample %>%
        setkey(cpf) %>%
        count_unique_by_group(name, by = .(cpf)) %>%
        summarise_duplicates()

    data_diagnostics <- list(data_diagnostics_name, data_diagnostics_cpf) %>%
        modify(~ mutate(., year = year)) %>%
        set_names(c("name", "cpf")) %>%
        bind_rows(.id = "type")

    return(data_diagnostics)
}