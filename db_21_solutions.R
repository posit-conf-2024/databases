library(DBI)
library(tidyverse)
requireNamespace("duckplyr")

arrow::write_parquet(nycflights13::flights, "flights.parquet")

# 1. From the Parquet file, compute a lazy dbplyr tables
#    showing the mean and median departure delay
#    for each month.

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

flights <- duckdb::tbl_file(con, "flights.parquet")

month_delay <-
  flights |>
  summarise(
    .by = month,
    mean_delay = mean(dep_delay),
    median_delay = median(dep_delay)
  )

month_delay

# 2. Compute the same data as duckplyr lazy data frames.

nycflights13::flights |>
  select(month, dep_delay) |>
  duckplyr::as_duckplyr_df() |>
  summarise(
    .by = month,
    mean_delay = mean(dep_delay),
    median_delay = median(dep_delay)
  )

# 3. Store this data as a Parquet file.

nycflights13::flights |>
  select(month, dep_delay) |>
  duckplyr::as_duckplyr_df() |>
  summarise(
    .by = month,
    mean_delay = mean(dep_delay),
    median_delay = median(dep_delay),
  ) |>
  duckplyr::df_to_parquet("delay-by-month.parquet")

# 4. Read the Parquet file and plot the data.

library(ggplot2)

duckplyr::duckplyr_df_from_parquet("delay-by-month.parquet") |>
  pivot_longer(cols = c(mean_delay, median_delay), names_to = "delay_type", values_to = "delay") |>
  ggplot(aes(x = month, y = delay, color = delay_type)) +
  geom_point() +
  geom_line() +
  labs(title = "Mean delay by month")
