# input: filiados (partisan affiliation) and rais (employment data)
# note that there are some duplicated names, but they are rare
# also note that the majority of party members have only one affiliation
# the majority of duplicates in RAIS is due to holding several jobs
# final output: unique id and partisanship per year per state

# across all these datasets give me every single distinct name
# let me see how many total names I need to think about
# tiered decision making process
# all possible names and some way of making inferences about deduplication
# best guess at the distinct names (use the id's)
# sequential numbering
# dictionary | hash table to go through each dataset
# set up a dictionary in python
# perhaps just keep it simple and do it with whether the name matches or not
# create a master linkage number and verify
# individual dataframes with unique identifiers
# deidentification problem and merging data problem
# dictionary with working id code

# weed out the exact matches
# and then use the fuzzy matching algorithm in the remainder
# scoring function
# block them before you get into the scoring function

# create a function that extracts the relevant records
# layers of identification.
# block matching with the dictionary
# here's the list of all names there.
# intersection of the list of numbers
# create a key that will allow me to merge the data
# problem of memory: only load memories, do block-matching
# sequences of strings: scoring functions (computationally expensive)
# blocked each sequence in camer, do it by blocks
# sometimes people transpose names
# mindset: weeding out comparisons.
# list all of the id codes 
# transform the entire database into a key-value database

# only one instance of each unique string (keep a serial id in every single dataset)
# be creative with this unique id number
# listing of every single distinct record in a separate data table
# customer_id (schema sql)
# creating keys, schemas, queries that I really need in the end (smaller, faster)

# proof of concept: abductive reasoning
# log how long it takes, estimate the number of comparisons.
# audit, look for mistakes, estimates

# sets of dictionaries
# read literature on record linkage (efficient algorithm, blocking)
# create hash tables: hashing function
# create a database from SQL too

# create this file that has this unique record that maps me to the right location
# every single name for rais, filiados across all datasets
# it should link me to the unique records
# create a dictionary that can add records (running list of any name it has seen before)
# dictionary add function that will add that record
# design fast algorithms
# do some experiments
# look out for problems in the code (efficiency gains): go ahead and sort

# think many datasets
# one dataset that links each name and unique id to the record
# one dataset that links each distinct name to the records


library(tidyverse)
library(data.table)
library(haven)
library(here)

source(here("source/utils/preprocess.R"))

year <- 2006:2016
path_out <- "data/clean/preprocessed"

fwrite <- partial(data.table::fwrite, compress = "gzip")

# ==============================================================================
print("clean-up filiados and write-out")
# ==============================================================================
filiados <- fread(
    "data/raw/filiado_cpf.csv",
    nrows = 1e4,
    integer64 = "character"
)

filiados %>%
    clean_filiados() %>%
    fwrite(
        here(path_out, "filiados_clean.csv.gz")
    )

print("clean-up rais and write-out")
path_rais_out <- here(path_out, sprintf("rais_clean_%s.csv.gz", year))

walk2(
    year,
    path_rais_out,
    ~ read_rais(
        year = .x
    ) %>%
        clean_rais() %>%
        fwrite(
            file = .y
        )
)

# most efficient solution is actually to create an rsqlite database