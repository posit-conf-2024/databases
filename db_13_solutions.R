# attach relevant packages
library(tidyverse)

# Connection -------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb())
dm::copy_dm_to(con, dm::dm_pixarfilms(), set_key_constraints = FALSE, temporary = FALSE)

# Lazy tables ------------------------------------------------------------------

pixar_films <- tbl(con, "pixar_films")
academy <- tbl(con, "academy")

# Downsizing on the database: Exercises ----------------------------------------

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

# `left_join()` --------------------------------------------------------------------

# 1. How many rows does the join between `academy` and `pixar_films` contain?
#    Try to find out without loading all the data into memory. Explain.

left_join(pixar_films, academy, join_by(film)) |>
  count()

count(academy)

# 2. Which films are not yet listed in the `academy` table? What does the
#    resulting SQL query look like?
#    - Hint: Use `anti_join()`

anti_join(pixar_films, academy, join_by(film))

# 3. Plot a bar chart with the number of awards won and nominated per year.
#    Compute as much as possible on the database.
#    - Hint: "Long form" or "wide form"?

academy_won_nominated <-
  academy |>
  filter(status %in% c("Nominated", "Won")) |>
  select(film, status)

per_year_won_nominated <-
  pixar_films |>
  transmute(film, year = year(release_date)) |>
  inner_join(academy_won_nominated, join_by(film)) |>
  count(year, status) |>
  collect()
per_year_won_nominated

ggplot(per_year_won_nominated, aes(x = year, y = n, fill = status)) +
  geom_col()

# dbDisconnect(con)
