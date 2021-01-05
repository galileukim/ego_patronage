# ==============================================================================
# input: sql database of party affiliation
# output: assess extent to which there is party dominance
# ==============================================================================
source(
    here::here("source/descriptive/modules/globals.R")
)

source(
    here("source/descriptive/modules/requirements.R")
)

partisan_summary <- read_rds(
    here("data/clean/summary/partisan_dominance.rds")
)

# ==============================================================================
# compute party dominance across different categories
# ==============================================================================
message("computing dominance...")

group_vars <- c(
    "cbo_group", "contract_type", "edu_category"
)

labels <- c(
    "occupation category", "type of contract", "education"
)

message("printing out plots")

plot_partisan <- pmap(
    list(
        data = partisan_summary,
        x = group_vars,
        xlab = labels
    ),
    gg_boxplot,
    y = partisanship
)

# ==============================================================================
# exporting plots and data
# ==============================================================================
message("exporting plots")
path_to_figs <- ifelse(
    isTRUE(debug),
    "paper/figures/partisanship/sample/",
    "paper/figures/partisanship/"
)

export_plots <- list(
    filename = sprintf(
        here(path_to_figs, "plot_partisanship_by_%s.pdf"),
        group_vars
        ),
    plot = plot_partisan
)

pwalk(
    export_plots,
    save_plot
)
