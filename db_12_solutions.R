# attach relevant packages
library(tidyverse)

# Connection -------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb())
dm::copy_dm_to(con, dm::dm_pixarfilms(), set_key_constraints = FALSE, temporary = FALSE)

# Lazy tables ------------------------------------------------------------------

pixar_films <- tbl(con, "pixar_films")

# Downsizing on the database: Exercises ----------------------------------------

# `select()` -------------------------------------------------------------------

pixar_films

# *  Find several ways to select the 3 first columns

select(pixar_films, 1:3)
select(pixar_films, number:release_date)
select(pixar_films, !4:ncol(pixar_films))

# *  What happens if you include the name of a variable multiple times in a `select()` call?

select(pixar_films, number, release_date, number)

# *  Select all columns that contain underscores (use `contains()`)

select(pixar_films, contains("_"))

# *  Use `all_of()` to select 2 columns of your choice

select(pixar_films, all_of(head(colnames(pixar_films), n = 2)))

# `filter()` -------------------------------------------------------------------

pixar_films

# Find all films that
# 1. Are rated "PG"

filter(pixar_films, film_rating == "PG")

# 2. Had a run time below 95

filter(pixar_films, run_time < 95)

# 3. Had a rating of "N/A" or "Not Rated"

filter(pixar_films, film_rating %in% c("N/A", "Not Rated"))

# 4. Were released after and including year 2020

filter(pixar_films, release_date >= as.Date("2020-01-01"))

# 5. Have a missing name (`film` column) or `run_time`

filter(pixar_films, is.na(film) | is.na(run_time))

# 6. Are a first sequel (the name ends with "2", as in "Toy Story 2")
#     - Hint: Bring the data into the R session before filtering

filter(collect(pixar_films), grepl("2$", film))

# `count()`, `summarize()`, `group_by()`, `ungroup()` --------------------------

pixar_films

# 1. How many films are stored in the table?

count(pixar_films)

# 2. How many films released after 2005 are stored in the table?

filter(pixar_films, release_date >= as.Date("2006-01-01")) |>
  count()

# 3. What is the total run time of all films?
#     - Hint: Use `summarize(sum(...))`, watch out for the warning

summarize(pixar_films, total_time = sum(run_time, na.rm = TRUE))

# 4. What is the total run time of all films, per rating?
#     - Hint: Use `group_by()` or `.by`

pixar_films |>
  summarize(.by = film_rating, total_time = sum(run_time, na.rm = TRUE))

# dbDisconnect(con)
