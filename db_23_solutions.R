# 1. Write code to create a partitioned dataset with the `flights` table,
#    partitioned by `origin`.
#        - Hint: The dataset only contains flights departing from New York City airports.

con_rw <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = "flights.duckdb",
  read_only = FALSE
)

DBI::dbExecute(con_rw, "DROP TABLE IF EXISTS flights;")

flights_duckdb <- dplyr::copy_to(
  con_rw,
  nycflights13::flights,
  name = "flights",
  temporary = FALSE,
  overwrite = TRUE
)

dplyr::tbl(con_rw, "flights") |>
  dplyr::distinct(origin)

# Method 1 ---------------------------------------------------------------------

# DB-agnositic

ewr <-
  dplyr::tbl(con_rw, "flights") |>
  dplyr::filter(origin == "EWR") |>
  dplyr::collect()

lga <-
  dplyr::tbl(con_rw, "flights") |>
  dplyr::filter(origin == "LGA") |>
  dplyr::collect()

jfk <-
  dplyr::tbl(con_rw, "flights") |>
  dplyr::filter(origin == "JFK") |>
  dplyr::collect()

purrr::walk2(
  list(ewr, lga, jfk),
  list("EWR", "LGA", "JFK"),
  function(x, y) {
    if (!fs::dir_exists("manual-partition-flights")) {
      fs::dir_create("manual-partition-flights")
    }
    out_path <- fs::dir_create(
      fs::path("manual-partition-flights", paste0("origin=", y))
    )
    duckplyr::df_to_parquet(
      x,
      fs::path(out_path, "part-0.parquet")
    )
  }
)

# Method 2 ---------------------------------------------------------------------

# If on DuckDB

DBI::dbExecute(
  con_rw,
  "COPY flights TO 'flights-partion' (FORMAT PARQUET, PARTITION_BY origin);"
)

# Method 3 ---------------------------------------------------------------------

# If on DuckDB

dplyr::tbl(con_rw, "flights") |>
  arrow::to_arrow() |>
  arrow::write_dataset("flights-partition-arrow", partitioning = "origin")
