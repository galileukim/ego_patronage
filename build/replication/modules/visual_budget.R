finbra <- read_data(
    "finbra",
    "finbra.rds"
)

plot_budget <- finbra %>%
    pivot_longer(
        -c(cod_ibge_6, year),
        names_to = "category",
        values_to = "value"
    ) %>%
    group_by(year, category) %>%
    summarise(
        value = sum(value, na.rm = T) / 1e9
    ) %>%
    filter(
        !str_detect(category, "total|legislative|industry")
    ) %>%
    ggplot(
        aes(
            year,
            value,
        )
    ) +
    geom_area(
        aes(
            fill = category
        )
    ) +
    scale_fill_discrete(
        name = "HELLO",
        labels = c("A", "B", "C")
    ) +
    labs(
        # fill = "Category",
        x = "Year",
        y = "Total (billions)"
    )

save_fig(
    plot_budget,
    "descriptive_budget.pdf"
)
