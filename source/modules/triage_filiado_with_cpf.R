source(
    here::here("source/modules/setup_preprocess.R")
)

debug <- TRUE
sample_size <- if_else(isTRUE(debug), 1e4, Inf)

fread <- partial(
    data.table::fread,
    integer64 = "character",
    nrows = sample_size
    )

candidate <- fread(
    here("data/clean/candidate_deduped.csv.gz")
)

filiado <- fread(
    here("data/clean/filiados_deduped.csv.gz")
)

filiado_with_cpf <- filiado %>%
    inner_join(
        candidate,
        by = c("elec_title")
    )

filiado_without_cpf <- filiado %>%
    anti_join(
        candidate,
        by = c("elec_title")
    )

file_names <- sprintf(
    here("data/clean/%s.csv.gz"),
    c("filiado_with_cpf", "filiado_without_cpf")
)

walk2(
    list(filiado_with_cpf, filiado_without_cpf),
    file_names,
    fwrite
)