# attach relevant packages
library(tidyverse)

### Downsizing on the database #################################################

# Connection -------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb())
dm::copy_dm_to(con, dm::dm_pixarfilms(), set_key_constraints = FALSE, temporary = FALSE)

# Lazy tables ------------------------------------------------------------------

pixar_films <- tbl(con, "pixar_films")
pixar_films

# Aggregation ------------------------------------------------------------------

pixar_films |>
  summarize(.by = film_rating, n = n())

# Shortcut
pixar_films |>
  count(film_rating)

# Computations happens on the database!
pixar_films |>
  count(film_rating) |>
  show_query()

# Bring the data into the R session
df_pixar_films_by_rating <-
  pixar_films |>
  count(film_rating) |>
  collect()
df_pixar_films_by_rating

# Immutable data: original data unchanged
pixar_films |>
  collect()

# Second lazy table --------------------------------------------------------------

academy <- tbl(con, "academy")

academy
academy |>
  count(status)

# Left join ------

academy |>
  left_join(pixar_films)

academy |>
  left_join(pixar_films, join_by(film))

academy |>
  left_join(pixar_films, join_by(film)) |>
  show_query()

# Join with prior computation ------

academy_won <-
  academy |>
  filter(status == "Won") |>
  count(film, name = "n_won")
academy_won

pixar_films |>
  left_join(academy_won, join_by(film))

academy_won |>
  right_join(pixar_films, join_by(film)) |>
  arrange(release_date)

academy_won |>
  right_join(pixar_films, join_by(film)) |>
  mutate(n_won = coalesce(n_won, 0L)) |>
  arrange(release_date)

pixar_films |>
  left_join(academy_won, join_by(film)) |>
  mutate(n_won = coalesce(n_won, 0L)) |>
  arrange(release_date) |>
  show_query()

# Caveat: tables must be on the same source ------------------------------------

try(
  academy |>
    left_join(pixarfilms::pixar_films, join_by(film))
)

academy |>
  left_join(pixarfilms::pixar_films, join_by(film), copy = TRUE)

academy |>
  left_join(pixarfilms::pixar_films, join_by(film), copy = TRUE) |>
  show_query()

try(
  pixarfilms::academy |>
    left_join(pixar_films, join_by(film))
)

pixarfilms::academy |>
  left_join(pixar_films, join_by(film), copy = TRUE)

pixar_films_db <-
  copy_to(con, pixarfilms::pixar_films)

academy |>
  left_join(pixar_films_db, join_by(film))


# Downsizing on the database: Exercises ----------------------------------------

# `count()`, `summarize()`, `group_by()`, `ungroup()` --------------------------

pixar_films

# 1. How many films are stored in the table?
# 2. How many films released after 2005 are stored in the table?
# 3. What is the total run time of all films?
#     - Hint: Use `summarize(sum(...))`, watch out for the warning
# 4. What is the total run time of all films, per rating?
#     - Hint: Use `group_by()` or `.by`

# `left_join()` --------------------------------------------------------------------

pixar_films |>
  left_join(academy, join_by(film))

# 1. How many rows does the join between `academy` and `pixar_films` contain?
#    Try to find out without loading all the data into memory. Explain.
# 2. Which films are not yet listed in the `academy` table? What does the
#    resulting SQL query look like?
#    - Hint: Use `anti_join()`
# 3. Plot a bar chart with the number of awards won and nominated per year.
#    Compute as much as possible on the database.
#    - Hint: "Long form" or "wide form"?
