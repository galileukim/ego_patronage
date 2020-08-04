# ==============================================================================
print("extracting filiados with cpf")
# ==============================================================================
rais_filiados_with_cpf <- rais %>%
    distinct(
        id_employee,
        cpf
    ) %>%
    inner_join(
        filiados,
        by = "cpf"
    )

# triage merged candidates from dataset
rais_filiados_with_cpf_ids <- rais_filiados_with_cpf %>%
    distinct(
        id_employee
    )

rais_merge <- rais %>%
    anti_join(
        rais_filiados_with_cpf_ids
    )