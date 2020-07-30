# proceed in two parts
# first triage workers with cpf
# merge remainder with fastLink
# note that there are some duplicated names, but they are rare
# also note that the majority of party members have only one affiliation
# the majority of duplicates in RAIS is due to holding several jobs
# final output: unique id and partisanship per year per state
year <- 2006
sample_size <- 1e5

fwrite <- partial(data.table::fwrite, compress = "gzip")

filiados <- fread(
    "data/raw/filiado_cpf.csv",
    nrows = sample_size,
    integer64 = "character"
) %>%
    clean_filiados()

rais <- read_rais(
    year,
    sample_size
) %>%
    clean_rais()

# ==============================================================================
print("extracting filiados with cpf")
# ==============================================================================
rais_filiados_with_cpf <- rais %>%
    distinct(
        id_employee,
        cpf
    ) %>%
    inner_join(
        filiados,
        by = "cpf"
    )

# triage merged candidates from dataset
rais_filiados_with_cpf_ids <- rais_filiados_with_cpf %>%
    distinct(
        id_employee
    )

rais_merge <- rais %>%
    anti_join(
        rais_filiados_with_cpf_ids
    )

# ==============================================================================
print("write-out clean data tables")
# ==============================================================================
rais_filiados_with_cpf %>%
    fwrite(
        here(
            sprintf("data/preprocess/rais_filiados_with_cpf_%s", year),
            ".csv.gz"
        )
    )

filiados_clean %>%
    fwrite(
        here("data/preprocess/filiados_clean")
    )

rais_merge %>%
    fwrite(
        here("data/clean/preprocess/rais_merge")
    )