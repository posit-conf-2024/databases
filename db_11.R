# attach relevant packages
library(tidyverse)
library(DBI)

### Reading whole tables from the database #####################################

# Connection -------------------------------------------------------------------

con <- dbConnect(duckdb::duckdb())
con

# Discover tables --------------------------------------------------------------

dbListTables(con)

# Populate database (normally done by other people) ---------------------------

# Magic: import tables into the database
dm::copy_dm_to(
  con,
  dm::dm_pixarfilms(),
  set_key_constraints = FALSE,
  temporary = FALSE
)

# Discover tables --------------------------------------------------------------

dbListTables(con)
dbListFields(con, "pixar_films")

# Read table -------------------------------------------------------------------

df_pixar_films <- dbReadTable(con, "pixar_films")
df_pixar_films
as_tibble(df_pixar_films)

# Execute queries --------------------------------------------------------------

dbGetQuery(con, "SELECT * FROM pixar_films")

sql <- "SELECT * FROM pixar_films WHERE release_date >= '2020-01-01'"
# sql <- r"(SELECT * FROM "pixar_films" WHERE "release_date" >= '2020-01-01')"
dbGetQuery(con, sql)

# Further pointers -------------------------------------------------------------

# Quoting identifiers
dbQuoteIdentifier(con, "academy")
dbQuoteIdentifier(con, "from")

# Quoting literals
dbQuoteLiteral(con, "Toy Story")
dbQuoteLiteral(con, as.Date("2020-01-01"))

# Paste queries with glue_sql()

# Parameterized queries
sql <- "SELECT count(*) FROM pixar_films WHERE release_date >= ?"
dbGetQuery(con, sql, params = list(as.Date("2020-01-01")))

# Reading tables: Exercises ----------------------------------------------------

con

# 1. Read the `academy` table.
# 2. Read all records from the `academy` table that correspond to awards won
#     - Hint: Use the query "SELECT * FROM academy WHERE status = 'Won'"
# 3. Use quoting and/or a query parameter to make the previous query more robust.
#     - Hint: `sql <- paste0("SELECT * FROM academy WHERE ", quoted_column, " = ?")`
