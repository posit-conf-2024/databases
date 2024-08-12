library(DBI)
library(tidyverse)
requireNamespace("duckplyr")

### Working with database dumps #################################################

# Create data -------------------------------------------------------------------

arrow::write_parquet(nycflights13::flights, "flights.parquet")

fs::file_size("flights.parquet")
object.size(nycflights13::flights)

# Processing the local data ----

# Read as tibble ----

df <- arrow::read_parquet("flights.parquet")
df

# Read as Arrow dataset ----

ds <- arrow::open_dataset("flights.parquet")
ds
ds |>
  count(year, month, day) |>
  collect()

# Register as duckdb lazy table ----

con_memory <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

tbl <- duckdb::tbl_file(con_memory, "flights.parquet")
tbl
class(tbl)

tbl |>
  count(year, month, day)

tbl |>
  count(year, month, day) |>
  filter(month == 1) |>
  explain()

# The future: Register as duckplyr lazy data frame ----

duckplyr_df <- duckplyr::duckplyr_df_from_parquet("flights.parquet")
class(duckplyr_df)

filtered <-
  duckplyr_df |>
  count(year, month, day) |>
  filter(month == 1)

filtered |>
  explain()

filtered

filtered |>
  explain()

duckplyr_df |>
  count(year, month, day) |>
  filter(month == 1L) |>
  explain()

# Create partitioned data ------------------------------------------------------------------

arrow::write_dataset(
  nycflights13::flights,
  "flights-part/",
  partitioning = c("year", "month")
)

fs::dir_tree("flights-part")

# Read partitioned data ------------------------------------------------------------------

tbl_part <- duckdb::tbl_query(
  con_memory,
  "read_parquet('flights-part/*/*/*.parquet', hive_partitioning = true)"
)
tbl_part
class(tbl_part)

tbl_part |>
  count(year, month, day)

tbl_part |>
  filter(month %in% 1:3) |>
  explain()

# Create CSV data ------------------------------------------------------------------------

readr::write_csv(nycflights13::flights, "flights.csv")

# Read CSV data --------------------------------------------------------------------------

tbl_csv <- duckdb::tbl_file(con_memory, "flights.csv")

tbl_csv |>
  count(year, month, day)

tbl_csv |>
  count(year, month, day) |>
  explain()

duckplyr_df_csv <- duckplyr::duckplyr_df_from_csv("flights.csv")

duckplyr_df_csv |>
  count(year, month, day)

duckplyr_df_csv |>
  count(year, month, day) |>
  explain()

# Create derived Parquet data with duckplyr ---------------------------------------------------------

duckplyr_df_csv |>
  count(year, month, day) |>
  duckplyr::df_to_parquet("flights-count.parquet")

fs::file_size("flights-count.parquet")

duckplyr_df_count <-
  duckplyr::duckplyr_df_from_parquet("flights-count.parquet")

duckplyr_df_count |>
  explain()

duckplyr_df_count

duckplyr_df_count |>
  explain()

# Exercises -------------------------------------------------------------------------

# 1. From the Parquet file, compute a lazy dbplyr tables
#    showing the mean and median departure delay
#    for each month.
# 2. Compute the same data as duckplyr lazy data frames.
# 3. Store this data as a Parquet file.
# 4. Read the Parquet file and plot the data.
