library(DBI)
library(dplyr)

### DuckDB + SQL showcase #######################################################

# Create data -------------------------------------------------------------------

arrow::write_parquet(nycflights13::flights, "flights.parquet")
con_memory <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
tbl <- duckdb::tbl_file(con_memory, "flights.parquet")

# Application: DBI <=> dbplyr and pivoting -------------------------------------------------

daily_flights_by_dest <-
  tbl |>
  count(year, month, day, dest)

daily_flights_by_dest

daily_flights_by_dest_sql <-
  daily_flights_by_dest |>
  dbplyr::sql_render()
daily_flights_by_dest_sql

pivot_sql <- paste0(
  "PIVOT (", daily_flights_by_dest_sql, ") ON dest USING SUM(n)"
)

as_tibble(dbGetQuery(con_memory, pivot_sql))

system.time(
  as_tibble(dbGetQuery(con_memory, pivot_sql))
)

system.time(
  nycflights13::flights |>
    count(year, month, day, dest) |>
    tidyr::pivot_wider(names_from = dest, values_from = n, values_fill = 0)
)

write_pivot_sql <- paste0(
  "COPY (", pivot_sql, ") TO 'pivot.parquet' (FORMAT PARQUET)"
)
dbExecute(con_memory, write_pivot_sql)

q_unpivot_dyn <-
  "(SELECT * FROM (
   UNPIVOT 'pivot.parquet'
   ON COLUMNS(* EXCLUDE (year, month, day))
   INTO NAME dest VALUE n))"
tbl(con_memory, from = q_unpivot_dyn)
