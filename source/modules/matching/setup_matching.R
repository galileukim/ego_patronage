# intention: merge partisanship data with rais data
# look at what brollo et al. have done
# not a lot of detail, seems to be simply a fuzzy matching algorithm
# test-run with fastLink

# take into account a few challenges:
# 1) duplicated entries
# 2) duplicated names
# solution: use command distinct for now
print("set-up matching of partisan affiliation with rais")

library(tidyverse)
library(haven)
library(data.table)
library(fastLink)
library(here)

sample_size <- 5e5
path_to_rais <- "/home/BRDATA/RAIS/"
