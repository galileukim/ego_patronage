library(data.table)
library(tidyverse)
library(here)

fread <- partial(
    data.table::fread,
    integer64 = "character",
    nrows = sample_size
    )

source(
    here("source/utils/preprocess.R")
)