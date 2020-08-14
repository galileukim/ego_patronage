# ---------------------------------------------------------------------------- #
# input: employee identifiers from rais (note that these can be duplicated)
# note: each file partitions the data by year-region
source(
    here::here("source/modules/setup_preprocess.R")
)

library(haven)

debug <- FALSE
sample_size <- ifelse(isTRUE(debug), 1e3, Inf)

# ---------------------------------------------------------------------------- #
id_path <- "/home/BRDATA/RAIS/rawtxt"
years <- 2003
rais_id_hash <- new.env(hash = TRUE)

for (i in seq_along(years)) {
    n_unique_ids <- rep(NA, length(years))
    t <- years[i]

# ---------------------------------------------------------------------------- #
    print(
        sprintf("start producing hash for year %s", t)
    )

    id_file_path <- list.files(
        sprintf("%s/%s/", id_path, t),
        full.names = T
    )

    id_file_path <- ifelse(isTRUE(debug), id_file_path[1], id_file_path)

    select_cols <- c(
        "CPF",
        if (t <= 2010) c("NOME", "MUNICIPIO")
        else c("Nome Trabalhador", "Município")
    )

    select_cols_lower <- str_to_lower(select_cols)

# ---------------------------------------------------------------------------- #
    print("import 7z files")
    rais_id <- map_dfr(
        id_file_path,
        ~ read_7z(., t, select = select_cols)
    )

# ---------------------------------------------------------------------------- #
    print("deduplicate names")
    rais_id_unique <- rais_id %>%
        rename_with( # note that cpf comes first: check select_cols
            ~ c("cpf", "name", "cod_ibge_6")
        ) %>%
        extract_unique_id(
            c("cpf", "name", "cod_ibge_6")
        ) %>%
        transmute(
            cod_ibge_6,
            year = t,
            cpf,
            name = clean_name(name)
        )

    n_unique_ids[i] <- nrow(rais_id_unique)

# ---------------------------------------------------------------------------- #
    print("extract id hash")
    rais_id_new_hash <- extract_new_hash(
        rais_id_unique,
        rais_id_hash
    )

    print("update id hash")
    list2env(
        rais_id_new_hash,
        envir = rais_id_hash
        )

# ---------------------------------------------------------------------------- #
    print("write out hash table")

    length(rais_id_hash)
    print(n_unique_ids)
    
    # tibble(
    #     cpf = names(rais_id_new_hash),
    #     name = values(rais_id_hash, USE.NAMES = F)
    # ) %>%
    #     as.data.table() %>%
    #     fwrite(
    #         sprintf(here("data/clean/id/rais_id_hash_%s.csv"), t)
    #     )

    rais_id_new_hash %>%
        write_rds(
            sprintf(here("data/clean/id/rais_hash_%s.rds"), t),
            compress = "gz"
        )

    rm(rais_id_unique, rais_id_new_hash)
    gc()
}

rais_id_hash %>%
    write_rds(
        here("data/clean/id/rais_hash_complete.rds"),
        compress = "gz"
    )

print("create rais hash table: complete!")