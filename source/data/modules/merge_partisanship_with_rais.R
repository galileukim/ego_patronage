# intention: merge partisanship data with rais data
# look at what brollo et al. have done
# not a lot of detail, seems to be simply a fuzzy matching algorithm
# test-run with fastLink

# take into account a few challenges:
# 1) duplicated entries
# 2) duplicated names
# solution: use command distinct for now
print("set-up")

library(tidyverse)
library(haven)
library(data.table)
library(fastLink)

sample_size <- 5e5
path_to_rais <- "/home/BRDATA/RAIS/"

# subset by year and state
print("perform block merge (state-year) with fastLink")

# years <- 2003:2019
years <- 2006
states <- filiados_clean_with_years %>%
    distinct(state) %>%
    pull() %>%
    sort() %>% sample(3)

for (i in seq_along(years)) {
    t <- years[i]

    rais_filiados_link <- map(
        states,
        ~ blocked_fastLink(
            t,
            .,
            rais_merge,
            filiados_merge
        )
    )

    result_diagnostics <- rais_filiados_link %>%
        map(
            ~ run_diagnostics_fastLink(
                .,
                rais_merge,
                filiados_merge
            )
        )

    matched_rais_filiados <- rais_filiados_link %>%
        map(
            ~ getMatches(
                dfA = rais_merge,
                dfB = filiados_merge,
                fl.out = .,
                combine.dfs = T
            )
        )

    # extract bureaucrat id's for partisan members
    filiados_with_employee_id <- matched_rais_filiados %>%
        select(
            cod_ibge_6,
            year,
            id_employee,
            elec_title,
            party,
            starts_with("date"),
        )

    filiados_with_employee_id %>%
        fwrite(
            sprintf(here("data/clean/filiados_with_id_%s.csv"), years[i])
        )
}