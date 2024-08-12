library(tibble)
library(dm)
library(DBI)

# 1. Explore <https://dm.cynkra.com> and the built-in data models
#     `dm_nycflights13()` and `dm_pixarfilms()`

dm_nycflights13() |>
  dm_draw()

dm_pixarfilms() |>
  dm_draw(view_type = "all")

# 2.

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

# 2. Given the table structure above, create a dm object setting suitable
#     PK and FK relationships and unique keys.
#     Each speaker is an attendee, each event has a venue and exactly one speaker.
#     The helper table event_attendees matches attendees to events.
#     - Hint: Use the `dm()` function to create a dm object from scratch
#     - Hint: Use a unique key on `speakers$event_name`
dm_conf_target <-
  dm(venue, event, attendee, speaker, event_attendee) |>
  dm_add_pk(venue, venue_id) |>
  dm_add_pk(event, event_id) |>
  dm_add_pk(speaker, speaker_name) |>
  dm_add_pk(attendee, attendee_name) |>
  dm_add_fk(speaker, event_id, event) |>
  dm_add_fk(event, venue_id, venue) |>
  dm_add_fk(speaker, speaker_name, attendee, attendee_name) |>
  dm_add_fk(event_attendee, event_id, event) |>
  dm_add_fk(event_attendee, attendee_name, attendee) |>
  dm_add_uk(speaker, event_id)

# 3. Draw the dm object
dm_conf_target |>
  dm_draw()

# 4. Color the tables (optional)
dm_conf_target |>
  dm_set_colors(
    blue = event,
    red = venue,
    green3 = speaker,
    seagreen = attendee,
  ) |>
  dm_draw()

# 5. Deploy the data model to a DuckDB database
con_rw <- dbConnect(duckdb::duckdb(), "posit-conf.duckdb")
dm_conf_target <- copy_dm_to(con_rw, dm_conf_target, temporary = FALSE)

dbListTables(con_rw)

dm_conf_target |>
  dm_get_tables()

dbDisconnect(con_rw)
