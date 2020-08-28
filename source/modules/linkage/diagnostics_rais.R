# input: a) rais data
# output: table of diagnostics for rais that identifies duplicate names
source(
    here::here("source/modules/setup_preprocess.R")
)

source(
    here("source/utils/diagnostics_linkage.R")
)

debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e5, Inf)

# ---------------------------------------------------------------------------- #
rais_id_path <- here("data/clean/id/rais_hash")

rais_id_files <- list.files(
    rais_id_path,
    full.names = T
)

rais_diagnostics <- rais_id_files %>%
    map_dfr(
        ~ fread(.) %>%
            generate_diagnostics_rais()
    )

rais_diagnostics %>%
    fwrite(
        here("data/clean/debug/rais_diagnostics.csv")
    )

# conclusion: there are 0.02 percent duplicated names per electoral title
# which correspond to 0.02 percent of all entries