
# attach relevant packages
library(tidyverse)
library(DBI)

# Connection -------------------------------------------------------------------

con <- dbConnect(duckdb::duckdb())
con

# Magic: import tables into the database
dm::copy_dm_to(
  con,
  dm::dm_pixarfilms(),
  set_key_constraints = FALSE,
  temporary = FALSE
)

# Reading tables: Exercises ----------------------------------------------------

# 1. List all columns from the `box_office` table.

dbListFields(con, "box_office")

# 2. Read the `academy` table.

dbReadTable(con, "academy")

# 3. Read all records from the `academy` table that correspond to awards won
#     - Hint: Use the query "SELECT * FROM academy WHERE status = 'Won'"

dbGetQuery(con, "SELECT * FROM academy WHERE status = 'Won'")

# 4. Use quoting and/or query parameters to stabilize the previous query.

dbGetQuery(con,
  paste(
    "SELECT * FROM", dbQuoteIdentifier(con, "academy"),
    "WHERE status = ?"
  ),
  params = list("Won")
)

# dbDisconnect(con)
