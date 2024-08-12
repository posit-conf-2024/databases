# attach relevant packages
library(tidyverse)
library(DBI)

### Extract, Transform, Load ###################################################

# Extract: Raw data ------------------------------------------------------------

pixar_films_raw <- pixarfilms::pixar_films

# Transform: Fix column type, extract sequel column ----------------------------

pixar_films_clean <-
  pixar_films_raw |>
  separate(film, into = c("franchise", "sequel"),
    sep = " (?=[0-9]+$)", fill = "right", remove = FALSE
  ) |>
  mutate(across(c(number, sequel), as.integer)) |>
  mutate(.by = franchise, sequel = if_else(is.na(sequel) & n() > 1, 1L, sequel))

# Exercises --------------------------------------------------------------------

# 1. Adapt the ETL workflow to convert the `run_time` column to a duration.

pixar_films_clean <-
  pixar_films_clean |>
  mutate(run_time = hms::hms(minutes = run_time))
pixar_films_clean

#    - Hint: Use `mutate()` with `hms::hms(minutes = ...)` .
# 2. Re-run the workflow.
