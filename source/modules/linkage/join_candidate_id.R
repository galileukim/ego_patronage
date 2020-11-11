# ---------------------------------------------------------------------------- #
print("set-up")
library(haven)
debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e4, Inf)

source(
    here::here("source/modules/setup_preprocess.R")
)

# ---------------------------------------------------------------------------- #
message("read-in data")
candidate <- fread(
    here("data/clean/id/candidate_deduped.csv.gz")
)

rais_id_employee <- read_dta(
    "/home/BRDATA/RAIS/id/RAIS_employee_identifiers.dta",
    col_select = c(id_employee, cpf),
    n_max = sample_size
) %>%
    as.data.table()

# fix cpf
rais_id_employee[, cpf := as.character(cpf)]

setkey(candidate, cpf)
setkey(rais_id_employee, cpf)

message("joining data")

candidate_id <- candidate %>%
    merge(
        rais_id_employee,
        by = "cpf",
        all = FALSE
    )

message(
    "there are ", nrow(candidate_id), " matches in the data, out of",
    nrow(candidate), " possible in the candidate data"
)

candidate_id %>%
    fwrite(
        here("data/clean/id/candidate_id.csv.gz"),
        compress = "gzip"
    )