library(data.table)
library(tidyverse)
library(here)

fread <- partial(
    data.table::fread,
    integer64 = "character",
    nrows = sample_size,
    fill = TRUE
    )

source(
    here("source/utils/preprocess.R")
)