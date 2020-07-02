# create descriptive visualizations for educational data
edu_global <- read_data(
  "worldbank",
  "edu_global.rds"
)

plot_enroll <- edu_global %>% 
  filter(!is.na(enrollment_rate_primary)) %>% 
  ggplot(
    aes(
      year,
      enrollment_rate_primary,
      col = type
    )
  ) +
  geom_line(
    linetype = 'solid'
  ) +
  geom_point() +
  annotate(
    "text",
    label = "Global average",
    size = 2,
    col = "grey65",
    2010, 85
  ) +
  labs(
    y = "Enrollment (percentage)",
    x = "Year"
  ) +
  theme(
    legend.position = "none"
  ) +
  coord_cartesian(
    ylim = c(75, 100),
    xlim = c(2000, 2016)
  )

plot_pisa <- edu_global %>% 
  filter(!is.na(pisa)) %>% 
  ggplot(
    aes(
      year,
      pisa,
      col = type
    )
  ) +
  geom_line(
    linetype = 'solid'
  ) +
  geom_point() +
  annotate(
    "text",
    label = "Global average",
    size = 2,
    col = "grey65",
    2011, 490
  ) +
  labs(
    y = "Pisa scores (average)",
    x = "Year"
  ) +
  theme(
    legend.position = "none"
  ) +
  coord_cartesian(
    xlim = c(2000, 2016),
    ylim = c(325, 500)
  )

plot_global <- cowplot::plot_grid(
  plot_enroll,
  plot_pisa,
  ncol = 2
)

ggsave(
  plot_global,
  filename = p_file_here('figs', 'plot_global.pdf')
)

plot_enroll <- edu_global %>%
  filter(type == "global") %>% 
  ggplot(
    aes(
      year,
      enrollment_rate_primary,
      col = "grey"
    )
  ) +
  geom_line(
    linetype = 'solid'
  ) +
  geom_point() +
  annotate(
    "text",
    label = "Global average",
    size = 2,
    col = "grey65",
    2010, 85
  ) +
  labs(
    y = "Enrollment (percentage)",
    x = "Year"
  ) +
  theme(
    legend.position = "none"
  ) +
  coord_cartesian(
    ylim = c(75, 100),
    xlim = c(2000, 2016)
  )

plot_pisa <- edu_global %>% 
  filter(type == "global") %>% 
  filter(!is.na(pisa)) %>% 
  ggplot(
    aes(
      year,
      pisa,
      col = "#FC8D62"
    )
  ) +
  geom_line(
    linetype = 'solid'
  ) +
  geom_point() +
  annotate(
    "text",
    label = "Global average",
    size = 2,
    col = "grey65",
    2011, 490
  ) +
  labs(
    y = "PISA scores (average)",
    x = "Year"
  ) +
  theme(
    legend.position = "none"
  ) +
  coord_cartesian(
    xlim = c(2000, 2016),
    ylim = c(325, 500)
  )

plot_edu_1 <- cowplot::plot_grid(
  plot_enroll,
  plot_pisa,
  ncol = 2
)

ggsave(
  plot_edu_1,
  filename = p_file_here('figs', 'plot_global_1.pdf')
)

plot_enroll_type <- edu_global %>%
  filter(!is.na(enrollment_rate_primary)) %>% 
  ggplot(
    aes(
      year,
      enrollment_rate_primary,
      color = type
    )
  ) +
  geom_line(
    linetype = 'solid'
  ) +
  geom_point() +
  labs(
    y = "Enrollment (percentage)",
    x = "Year"
  ) +
  theme(
    legend.position = "none"
  ) +
  scale_color_manual(
    values = c("global" = "#66C2A5", "br" = "#FC8D62")
  ) +
  coord_cartesian(
    ylim = c(75, 100),
    xlim = c(2000, 2016)
  ) +
  annotate(
    "text",
    label = "Brazil",
    size = 2,
    col = "grey65",
    2012, 100
  ) +
  annotate(
    "text",
    label = "Global average",
    size = 2,
    col = "grey65",
    2010, 85
  ) 

plot_pisa_type <- edu_global %>% 
  filter(!is.na(pisa)) %>% 
  ggplot(
    aes(
      year,
      pisa,
      col = type,
    )
  ) +
  geom_line(
    linetype = 'solid'
  ) +
  geom_point() +
  labs(
    y = "PISA scores (average)",
    x = "Year"
  ) +
  theme(
    legend.position = "none"
  ) +
  scale_color_manual(
    values = c("global" = "#66C2A5", "br" = "#FC8D62")
  ) +
  annotate(
    "text",
    label = "Brazil",
    size = 2,
    col = "grey65",
    2012, 380
  ) +
  annotate(
    "text",
    label = "Global average",
    size = 2,
    col = "grey65",
    2011, 490
  ) +
  coord_cartesian(
    xlim = c(2000, 2016),
    ylim = c(325, 500)
  )

plot_edu_2 <- cowplot::plot_grid(
  plot_enroll_type,
  plot_pisa_type,
  ncol = 2
)

ggsave(
  plot_edu_2,
  filename = p_file_here('figs', 'plot_global_2.pdf')
)