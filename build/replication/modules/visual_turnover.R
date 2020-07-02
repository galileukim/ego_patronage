# generate plots to visualize turnover in educational staff: teacher and school principals
# 1) ensure that each task compiles
# import data
rais_edu <- read_data(
    "rais",
    "rais_mun_edu.rds"
)

censo_school_turnover <- read_data(
    "censo_escolar",
    "censo_school_turnover.rds"
)

# visualization of differential turnovers by type of educational staff
plot_turnover_edu <- rais_edu %>%
    filter(year >= 2000) %>%
    group_by(
        rais_category,
        year
    ) %>%
    summarise_prop(
        "rais_hired"
    ) %>%
    ggplot(
        aes(
            year,
            prop,
            col = rais_category,
            pch = rais_category
        )
    ) +
    geom_point(
        size = 4
    ) +
    facet_wrap(
        . ~ rais_category,
        labeller = labeller(
            rais_category = c(principal = "Principal", teacher = "Teacher")
        ),
        ncol = 1
    ) +
    mandate_year(
        seq(2001, 2013, 4)
    ) +
    theme(
        legend.position = "bottom",
        legend.title = element_blank()
    ) +
    labs(
        x = "Year",
        y = "Percentage"
    )

save_fig(
    plot_turnover_edu,
    "plot_turnover_edu.pdf"
)

# distribution of teacher turnover index
hist_turnover_index <- censo_school_turnover %>%
    filter(
        between(year, 2007, 2015) &
        n >= 5
    ) %>%
    ggplot(
        aes(
            turnover_index,
            # y = stat(width * density),
            group = year,
            color = year
        )
    ) +
    geom_density() +
    scale_y_continuous(
        labels = scales::percent_format()
    ) +
    labs(
        x = "Distribution of turnover index",
        y = "Count"
    )

save_fig(
    hist_turnover_index,
    "hist_turnover_index.pdf"
)


patronage_category <- patronage %>%
    mutate(
        year = as.integer(year)
    ) %>%
    filter(
        cbo_category %in% c("administration", "education", "healthcare", "services")
    )

patronage_category %>%
    group_by(year, cbo_category) %>%
    summarise(
        prop_hired = sum(prop_hired * total) / sum(total)
    ) %>%
    ungroup() %>%
    ggplot(
        aes(year, prop_hired, group = cbo_category, color = cbo_category)
    ) +
    labs(x = "year", y = "proportion of hires") +
    geom_line() +
    geom_point(
        size = 2
    ) +
    geom_vline(
        xintercept = seq(2005, 2013, 4),
        linetype = "dotted"
    ) +
    scale_x_continuous(
        breaks = seq(2003, 2016, 2)
    ) +
    ggsave(
        p_file_here(
            "figs", "hire_by_category"
        )
    )


# address alternative theories
# population
patronage_category %>%
    gg_summary(
        censo_pop,
        prop_hired
    ) +
    scale_x_log10(
        breaks = scales::trans_breaks("log10", function(x) 10^x),
        labels = scales::trans_format("log10", scales::math_format(10^.x))
    ) +
    facet_wrap(
        cbo_category ~ .
    ) +
    coord_cartesian(
        ylim = c(0, 0.2)
    )

# programmatic party
patronage %>%
    mutate(
        mayor_party = if_else(
            mayor_party %in% c("pt", "psdb", "pmdb"),
            mayor_party,
            "other"
        )
    ) %>%
    ggplot(
        aes(
            mayor_party,
            prop_hired
        )
    ) +
    stat_summary(
        fun.data = "mean_cl_boot",
        color = matte_indigo
    )

# contract type
# rais_contract <- DBI::dbGetQuery(
#   con,
#   "
#     SELECT cod_ibge_6, year, AVG(CAST(adm = '01' OR adm = '02' AS INTEGER)) AS hired,
#     AVG(CAST(contract_type = 'permanent high' AS INTEGER)) AS permanent
#     FROM (SELECT cod_ibge_6, year, adm, contract_type FROM rais_mun WHERE year > 2002) db
#     GROUP BY cod_ibge_6, year;
#     "
# )

# rural
patronage_category %>%
    gg_summary(
        censo_rural,
        prop_hired
    ) +
    facet_wrap(
        cbo_category ~ .
    ) +
    coord_cartesian(
        ylim = c(0, 0.3)
    )

# level of economic development
patronage_category %>%
    gg_summary(
        censo_median_wage,
        prop_hired
    ) +
    facet_wrap(
        cbo_category ~ .
    ) +
    coord_cartesian(
        ylim = c(0, 0.3)
    )

# illiteracy
patronage_category %>%
    gg_summary(
        1 - censo_lit_rate,
        prop_hired
    ) +
    facet_wrap(
        cbo_category ~ .
    ) +
    labs(
        x = "illiteracy rate",
        y = "proportion of hires"
    )