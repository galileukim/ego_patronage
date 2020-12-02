# input: a) filiados data
# output: table of diagnostics for filiados that identifies duplicate names
source(
    here::here("source/modules/setup_preprocess.R")
)

source(
    here("source/utils/diagnostics_linkage.R")
)

debug <- TRUE
sample_size <- ifelse(isTRUE(debug), 1e5, Inf)
between <- data.table::between

source(
    here("source/modules/linkage/preprocess_data.R")
)

# ---------------------------------------------------------------------------- #
# how many duplicated names are there?
filiados_diagnostics_name <- filiados %>%
    setkey(name) %>%
    count_dt(name) %>%
    summarise_duplicates()

# conclusion: there are 12.4 percent duplicated names
# which correspond to 9 percent of total entries

filiados_diagnostics_title <- filiados %>%
    setkey(electoral_title) %>%
    count_unique_by_group(name, by = .(electoral_title)) %>%
    summarise_duplicates()

# conclusion: there are 0.02 percent duplicated names per electoral title
# which correspond to 0.02 percent of all entries