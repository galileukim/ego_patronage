build_from_source <- function(...) source(here("source", ...))

module_deduplication <- c(
    "dedupe_candidates", 
    "dedupe_filiados", 
    "dedupe_rais", 
    "triage_filiado_with_cpf", 
    "record_linkage",
    "record_linkage_mun"
)

module_deduplication <- paste0("deduplication/", module_deduplication, ".R") 

module_deduplication %>%
    walk(build_from_source)