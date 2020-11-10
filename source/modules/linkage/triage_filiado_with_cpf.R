print("set-up")
debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e4, Inf)

source(
    here::here("source/modules/setup_preprocess.R")
)

print("read-in data")
candidate <- fread(
    here("data/clean/id/candidate_deduped.csv.gz")
) %>%
    select(-name)

filiado <- fread(
    here("data/clean/id/filiado_deduped.csv.gz")
)

print("joining data files")
filiado_with_cpf <- filiado %>%
    inner_join(
        candidate,
        by = c("electoral_title")
    )

filiado_without_cpf <- filiado %>%
    anti_join(
        candidate,
        by = c("electoral_title")
    )

print("write-out data")
file_names <- sprintf(
    here("data/clean/id/%s.csv.gz"),
    c("filiado_id_with_cpf", "filiado_id_without_cpf")
)

walk2(
    list(filiado_with_cpf, filiado_without_cpf),
    file_names,
    fwrite
)