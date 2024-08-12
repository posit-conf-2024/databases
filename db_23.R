library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)

### Database dumps #############################################################

# Connection -------------------------------------------------------------------

if (fs::file_exists("flights.duckdb")) {
  fs::file_delete("flights.duckdb")
}

con_rw <- dbConnect(duckdb::duckdb(), dbdir = "flights.duckdb")
flights_duckdb <- copy_to(
  con_rw,
  nycflights13::flights,
  name = "flights",
  temporary = FALSE
)
dbDisconnect(con_rw)

# Exploration ----

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "flights.duckdb",
  read_only = TRUE
)
flights_duckdb <- tbl(con, "flights")

# Method 1: via local data frame ----

flights_duckdb |>
  filter(month == 1) |>
  collect() |>
  duckplyr::df_to_parquet("flights-jan.parquet")

flights_duckdb |>
  collect() |>
  duckplyr::df_to_parquet("flights.parquet")

# Method 2: via DBI ----

sql_jan <- flights_duckdb |>
  filter(month == 1) |>
  dbplyr::sql_render()

fs::dir_create("flights-arrow")

res <- dbSendQuery(con, sql_jan)
i <- 0
repeat {
  df <- dbFetch(res, n = 10000)
  if (nrow(df) == 0) break
  path <- fs::path("flights-arrow", sprintf("part-%05d.parquet", i))
  duckplyr::df_to_parquet(df, path)
  i <- i + 1
  message("Written ", nrow(df), " rows to ", path)
}
dbClearResult(res)

fs::dir_tree("flights-arrow/")

# Method 3: via parquetize ----

parquetize::dbi_to_parquet(
  con,
  sql_jan,
  "flights-parquetized",
  max_rows = 10000
)

fs::dir_tree("flights-parquetized/")

# Method 4: via DBI and arrow ----

con_adbi <- dbConnect(
  adbi::adbi(duckdb::duckdb_adbc()),
  path = "flights.duckdb"
)

sql <- "SELECT * FROM flights"

res <- dbSendQueryArrow(con_adbi, sql)
stream <- dbFetchArrow(res)
arrow::write_dataset(
  arrow::as_record_batch_reader(stream),
  "flights-adbi/"
)
dbClearResult(res)

# Partitions ----

nycflights13::flights |>
  arrow::write_dataset(
    "flights-part-arrow/",
    partitioning = "month"
  )

fs::dir_tree("flights-part-arrow/")

# Adding partitions to a dataset ----

write_month <- function(month) {
  sql <- flights_duckdb |>
    filter(month == !!month) |>
    dbplyr::sql_render()

  dir <- fs::path(
    "flights-part-manual",
    sprintf("month=%d", month)
  )
  fs::dir_create(dir)

  df <- dbGetQuery(con, sql)
  duckplyr::df_to_parquet(
    df,
    fs::path(dir, "part-0.parquet")
  )
}

write_month(1)
write_month(2)
write_month(3)

fs::dir_tree("flights-part-manual")

# Exercises -------------------------------------------------------------------------



# 1. Write code to create a partitioned dataset with the `flights` table,
#    partitioned by `origin`.
#        - Hint: The dataset only contains flights departing from New York City airports.
