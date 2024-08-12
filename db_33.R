# attach relevant packages
library(tidyverse)
library(dm)

# display chosen presentation (it might take a few seconds to appear)
slide_viewer <- function(path) {
  tmp <- tempfile(fileext = ".html")
  file.copy(path, tmp)
  rstudioapi::viewer(tmp)
}
# slide_viewer("materials/databases.html")

### Data models ################################################################

# Data model objects -----

pixar_dm <- dm_pixarfilms()
pixar_dm

pixar_dm |>
  dm_draw()

names(pixar_dm)

pixar_dm$pixar_films
pixar_dm$academy

pixar_dm |>
  dm_get_tables()

# Showcase: wrapping all tables in a data model:
pixar_films_wrapped <-
  pixar_dm |>
  dm_wrap_tbl(pixar_films) |>
  pull_tbl(pixar_films)

pixar_films_wrapped
pixar_films_wrapped$academy[1:2]


### Keys, constraints, normalization ###########################################

# Data model object ------

pixar_dm <- dm_pixarfilms()

# Primary keys ----

any(duplicated(pixar_dm$pixar_films$film))
check_key(pixar_dm$pixar_films, film)
any(duplicated(pixar_dm$academy[c("film", "award_type")]))
check_key(pixar_dm$academy, film, award_type)
try(
  check_key(pixar_dm$academy, film)
)

# Foreign keys ----

all(pixar_dm$academy$film %in% pixar_dm$pixar_films$film)
check_subset(pixar_dm$academy, film, pixar_dm$pixar_films, film)
try(
  check_subset(pixar_dm$pixar_films, film, pixar_dm$academy, film)
)

# Constraints ----

pixar_dm |>
  dm_examine_constraints()

dm_pixarfilms(consistent = TRUE) |>
  dm_examine_constraints()

dm_nycflights13() |>
  dm_examine_constraints()

# Joins ----

pixar_dm |>
  dm_zoom_to(academy)

# With zooming:
pixar_dm |>
  dm_zoom_to(academy) |>
  left_join(pixar_films, select = c(film, release_date))

# With flattening:
pixar_dm |>
  dm_flatten_to_tbl(academy)

dm_nycflights13() |>
  dm_select(weather, -year, -month, -day, -hour) |>
  dm_flatten_to_tbl(flights)

# Joining is easy, leave the tables separate for as long as possible!

# Exercises --------------------------------------------------------------------

venue <- tibble(
  venue_id = character(),
  floor = character(),
  capacity = integer(),
)

event <- tibble(
  event_id = character(),
  event_name = character(),
  event_type = character(),
  venue_id = character(),
  date_start = vctrs::new_datetime(),
  date_end = vctrs::new_datetime(),
)

attendee <- tibble(
  attendee_name = character(),
  favorite_package = character(),
)

speaker <- tibble(
  speaker_name = character(),
  event_id = character(),
)

event_attendee <- tibble(
  event_id = character(),
  attendee_name = character(),
)

# 1. Explore <https://dm.cynkra.com> and the built-in data models
#     `dm_nycflights13()` and `dm_pixarfilms()`
# 2. Given the table structure above, create a dm object setting suitable
#     PK and FK relationships and unique keys.
#     Each speaker is an attendee, each event has a venue and exactly one speaker.
#     The helper table event_attendees matches attendees to events.
#     - Hint: Use the `dm()` function to create a dm object from scratch
#     - Hint: Use a unique key on `speakers$event_name`
# 3. Draw the dm object
# 4. Colour the tables
#     - Hint: Review colors at <https://rpubs.com/krlmlr/colors>
# 5. Deploy the data model to a DuckDB database
