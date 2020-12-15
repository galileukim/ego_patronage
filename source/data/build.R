# this script is divided into two main components:
# 1) provide a linkage between party affiliation data and rais
# to extract anonymized identifiers and use them to analyze political careers
# 2) use of the party affiliation data to explore careers
# in the bureaucracy

# 1) join partisan affiliation to rais identifiers
build_from_source <- function(...) source(
    here::here("source/data/modules/", ...)
)

build_from_source("setup_preprocess.R")

# ---------------------------------------------------------------------------- #
module_deduplication <- c(
    "dedupe_candidates", 
    "dedupe_filiados", 
    "dedupe_rais"
)

module_deduplication <- sprintf(
    "record_linkage/deduplication/%s.R",
    module_deduplication
)

module_deduplication %>%
    walk(build_from_source)

# ---------------------------------------------------------------------------- #
module_linkage <- c(
    "triage_filiado_with_cpf",
    "create_record_linkage_mun",
    "create_record_linkage_state",
    # "create_record_linkage_fastLink",
    "join_hash_table_with_id_employee"
)

module_linkage <- sprintf(
"record_linkage/linkage/%s.R",
    module_linkage
)

module_linkage %>%
    walk(build_from_source)

# ---------------------------------------------------------------------------- #
build_from_source("database", "requirements.R")

module_database <- c(
    "create_rais_sql",
    "upload_filiado_sql",
    "upload_additional_data",
    "generate_municipal_tables",
    "generate_bureaucrat_tables"
)

module_database <- sprintf(
    "database/%s.R",
    module_linkage
)

module_database %>%
    walk(build_from_source)