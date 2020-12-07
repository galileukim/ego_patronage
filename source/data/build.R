# this script is divided into two main components
# the first one is to provide a linkage between party affiliation data and rais
# to extract anonymized identifiers and use them to analyze political careers
# thes second one is the use of the party affiliation data to explore careers
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
    "record_link_filiado",
    "structure_sql"
)

module_selection <- c(
    "identify_municipal_bureaucrats"
)