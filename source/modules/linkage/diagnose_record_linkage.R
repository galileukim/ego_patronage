sample_diagnostic <- rais_filiados %>% 
    sample_n(1e5)

sample_diagnostic %>%
    group_by(cod_ibge_6, cpf, electoral_title) %>%
    mutate(n())