# attach relevant packages
library(tidyverse)
library(DBI)

### Extract, Transform, Load ###################################################

# Extract: Raw data ------------------------------------------------------------

pixar_films_raw <- pixarfilms::pixar_films
pixar_films_raw

# Transform: Fix column type, extract sequel column ----------------------------

pixar_films_clean <-
  pixar_films_raw |>
  separate(film, into = c("franchise", "sequel"),
    sep = " (?=[0-9]+$)", fill = "right", remove = FALSE
  ) |>
  mutate(across(c(number, sequel), as.integer)) |>
  mutate(.by = franchise, sequel = if_else(is.na(sequel) & n() > 1, 1L, sequel))
pixar_films_clean

# Create target database -------------------------------------------------------

if (fs::file_exists("pixar.duckdb")) {
  fs::file_delete("pixar.duckdb")
}

# Load: Write table to the database --------------------------------------------

con_rw <- dbConnect(duckdb::duckdb(), dbdir = "pixar.duckdb")
con_rw

if (!dbExistsTable(con_rw, "pixar_films")) {
  dbWriteTable(con_rw, "pixar_films", pixar_films_clean)
  dbExecute(con_rw, "CREATE UNIQUE INDEX pixarfilms_pk ON pixar_films (film)")
}

dbDisconnect(con_rw)

# Reload: Write table to the database if the table exists ----------------------------------

con_rw <- dbConnect(duckdb::duckdb(), dbdir = "pixar.duckdb")
con_rw

dbExecute(con_rw, "TRUNCATE TABLE pixar_films")
dbAppendTable(con_rw, "pixar_films", pixar_films_clean)

dbDisconnect(con_rw)

# Consume: share the file, open it ---------------------------------------------

con <- dbConnect(duckdb::duckdb(), dbdir = "pixar.duckdb")
my_pixar_films <- tbl(con, "pixar_films")
my_pixar_films

# Exercises --------------------------------------------------------------------

pixar_films_raw

# 1. Adapt the ETL workflow to convert the `run_time` column to a duration.
#    - Hint: Use `mutate()` with `hms::hms(minutes = ...)` .
# 2. Re-run the workflow.

dbDisconnect(con)
