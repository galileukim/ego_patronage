# this script is divided into two main components
# the first one is to provide a linkage between party affiliation data and rais
# to extract anonymized identifiers and use them to analyze political careers
# thes second one is the use of the party affiliation data to explore careers
# in the bureaucracy

# 1) join partisan affiliation to rais identifiers
build_from_source <- function(...) source(
    here::here("source/modules/", ...)
)

build_from_source("setup_preprocess.R")

module_deduplication <- c(
    "dedupe_candidates", 
    "dedupe_filiados", 
    "dedupe_rais"
)

module_deduplication <- paste0("deduplication/", module_deduplication, ".R") 

module_deduplication %>%
    walk(build_from_source)

module_linkage <- c(
    "triage_filiado_with_cpf",
    "create_record_linkage_mun",
    "create_record_linkage_state",
    # "create_record_linkage_fastLink",
    "join_hash_table_with_id_employee"
)

module_linkage <- sprintf(
    "linkage/%s.R",
    module_linkage
)

module_linkage %>%
    walk(build_from_source)


