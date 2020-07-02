election <- read_data(
    "tse",
    "election.rds"
)

censo <- read_data(
    "censo_br",
    "censo_2000.rds"
)

rais <- read_data(
    "rais",
    "rais_mun_edu.rds"
)

# 1) visualize underlying distribution of coalition shares
hist_coalition <- election %>%
    gg_histogram(
        aes(coalition_share * 100),
        bins = 20
    ) +
    labs(
        x = "Share of seats (percentage)",
        y = "Count"
    )

save_fig(
    hist_coalition,
    "hist_coalition.pdf"
)

# 2) add additional context on how these are structured (size, composition)
# address the problem of confounding even if partially
covs <- quo(
    c(
        coalition_share,
        chamber_size,
        censo_pop,
        censo_rural,
        censo_median_wage,
        rais_size,
        rais_higher_edu,
        rais_permanent,
        rais_wage
    )
)

rais_mun <- rais %>%
    group_summarise(
        c(cod_ibge_6, election_year),
        c(rais_size, rais_higher_edu, rais_permanent, rais_wage),
        ~ mean(., na.rm = T)
    )

election_covariate <- election %>%
    left_join(
        censo,
        by = "cod_ibge_6"
    ) %>%
    left_join(
        rais_mun,
        by = c("cod_ibge_6", "election_year")
    ) %>%
    select(
        {{covs}}
    ) %>%
    na.omit()

cov_balance <- cobalt::bal.tab(
    treat = election_covariate %>% pull(coalition_share),
    covs = election_covariate %>% select({{covs}}, -coalition_share),
    thresholds = c(m = 0.1)
)

love_plot <- cobalt::love.plot(
    cov_balance,
    threshold = 0.1,
    title = NULL,
    colors = matte_indigo,
    position = "none"
) +
    scale_y_discrete(
        labels = c(
            "chamber_size" = "chamber size",
            "censo_pop" = "population",
            "censo_rural" = "rural population",
            "censo_median_wage" = "median municipal wage",
            "rais_size" = "educational staff size",
            "rais_higher_edu" = "prop. of staff with higher education",
            "rais_permanent" = "prop. of staff with tenured contract",
            "rais_wage" = "median staff salary"
        ),
        position = "right"
    ) +
    coord_cartesian(
        xlim = c(-0.35, 0.35)
    )

save_fig(
    love_plot,
    "covariate_balance.pdf"
)