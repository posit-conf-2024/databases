
# attach relevant packages
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

# 1. List all columns from the `pixar_films` table.

dbListFields(con, "pixar_films")

# 2. Review the help for `dbListFields()` and `dbListTables()`,
#    and the index on <https://dbi.r-dbi.org/reference/>.

?dbListFields
?dbListTables
browseURL("https://dbi.r-dbi.org/reference/")
