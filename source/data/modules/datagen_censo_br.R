censo_files <- list.files(
  here("data", "raw", "censo_br")
)

for(i in seq_along(censo_files)){
  censo <- censo_files[i] %>%
    fread %>%
    transmute(
      cod_ibge_6,
      median_wage,
      rural,
      lit_rate,
      sanitation,
      garbage,
      light,
      pop,
      student_age,
      log_pop = log(pop)
    ) %>%
    setNames(
      c(
        "cod_ibge_6",
        paste0("censo_", names(.)[2:length(names(.))])
      )
    )
  
  # write-out
  censo %>% 
    write_data(
      "censo_br", 
      paste0("censo_", c(2000, 2010)[i], ".rds")
    )
}