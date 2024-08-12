# attach relevant packages
library(DBI)

### First steps ################################################################

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
dbListFields(con, "box_office")

# First steps: Exercises -------------------------------------------------------

con

# 1. List all columns from the `pixar_films` table.
# 2. Review the help for `dbListFields()` and `dbListTables()`,
#    and the index on <https://dbi.r-dbi.org/reference/>.
