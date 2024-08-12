# attach relevant packages
library(DBI)
library(dm)

### Remote databases ###################################################

# Connect --------------------------------------------------------------

con <- dbConnect(
  RMariaDB::MariaDB(),
  dbname = "CORA",
  username = "guest",
  password = "ctu-relational",
  host = "relational.fel.cvut.cz"
)

# List tables ----------------------------------------------------------

dbListTables(con)

# Use dm for many tables -----------------------------------------------

dm <- dm_from_con(con)

dm

dm |>
  dm_nrow()

dm$paper

dm |>
  dm_get_tables()
