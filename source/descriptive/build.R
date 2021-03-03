# ==============================================================================
# descriptive statistics of turnover of party affiliates
# ==============================================================================
build_from_source("descriptive", "requirements.R")

module_selection <- c(
    "party_membership",
    "party_contribution",
    "bureau_partisanship",
    "prior_to_bureaucy",
    "entry_into_bureaucracy",
    "partisan_vs_non_partisan"
)