year_to_char <- function(data) {
  data <- data %>%
    mutate_at(
      vars(year),
      as.character
    )

  return(data)
}

fix_wage <- function(data) {
  data %>%
    mutate(
      wage = wage * 0.573395 / (cpi / 100)
    ) %>%
    select(-cpi)
}

fix_contract <- function(data) {
  data %>%
    mutate(
      contract_type = case_when(
        contract <= 25 ~ "regular",
        between(contract, 30, 35) ~ "career",
        between(contract, 40, 50) ~ "temporary",
        TRUE ~ "other"
      )
    )
}

fix_occupation <- function(data) {
  data %>%
    mutate(
      cbo_group = recode(
        occupation,
        `1` = "bureaucrat_high",
        `2` = "frontline_high",
        `3` = "frontline_high",
        `4` = "bureaucrat_low",
        `5` = "frontline_low",
        .default = "other"
      ),
      cbo_group_detail = recode(
        occupation,
        `0` = "army",
        `1` = "executive",
        `2` = "liberal arts",
        `3` = "engineer",
        `4` = "administration",
        `5` = "services",
        `6` = "agriculture",
        `7` = "industry",
        `8` = "industry",
        `9` = "maintenance"
      )
    )
}

trim_rais <- function(data, ...) {
  data %>%
    transmute(
      id_employee,
      name = nome,
      year,
      cod_ibge_6 = municipio,
      cbo_02 = cbo2002,
      age,
      edu,
      gender = genero,
      race = racacor,
      work_experience = tempoempr,
      wage = vlremmedia,
      hired = if_else(tipoadmissao == 1 | tipoadmissao == 2, 1, 0),
      fired = if_else(causadesli == 10 | causadesli == 11, 1, 0),
      departure = if_else(causadesli == 20 | causadesli == 21, 1, 0),
      date_admission,
      date_separation,
      contract = tipovinculo,
      nat_jur = naturjur,
      municipal = if_else(naturjur == 1031, 1, 0),
      outcome = recode(fired, `0` = 0, `10` = 2, `11` = 2, .default = 1),
      state = substr(cod_ibge_6, 1, 2),
      cbo_02 = ifelse(nchar(cbo_02) == 5, paste0("0", cbo_02), cbo_02),
      occupation = as.integer(substr(cbo_02, 1, 1)),
      hours = horascontr,
      cpi,
      ...
    )
}