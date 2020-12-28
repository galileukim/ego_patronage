# ==============================================================================
# descriptive statistics of turnover of party affiliates
# ==============================================================================
build_from_source("descriptive", "requirements.R")

module_selection <- c(
    "party_membership",
    "bureau_partisanship",
    "prior_to_bureaucy",
    "turnover_cycles",
    "partisan_vs_non_partisan"
)   