build_from_source <- function(...) source(here("source", ...))

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
    "join_hash_table_with_id_employee"
)

module_linkage <- sprintf(
    "deduplication/%s.R",
    module_linkage
)

module_linkage %>%
    walk(build_from_sourcez)