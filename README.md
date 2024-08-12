Databases with R
================

### posit::conf(2024)

by Kirill Müller

-----

:spiral_calendar: August 12, 2024  
:alarm_clock:     09:00 - 17:00  
:hotel:           402 Chiliwack
:writing_hand:    [pos.it/conf](http://pos.it/conf)

-----

## Description

As a data professional, you likely have to deal with databases that are larger than your available RAM.
Downloading the data requires patience, applying traditional workflows is frustrating.
This workshop will teach you to work with your (large) data:

-   if it resides in a traditional database, effortlessly

-   from local storage, using DuckDB, a modern database engine tailored to data analysis

The workshop will introduce basic database concepts and move on with practical work with traditional databases and DuckDB.
You are encouraged to bring your own data(base) to immediately apply what you have learned during the workshop.
Among others, the workshop showcases the DBI, dbplyr, duckdb, duckplyr, and dm packages.

## Audience

This course is for you if you:

-   have worked with the dplyr package.

-   have just read or heard about databases and are ready to get your hands dirty.

-   performed basic operations on a database, and you would like to deepen your knowledge.

-   have heard about DuckDB and want to know what makes it unique and how to leverage it in your daily workflow.

## Prework

#### Laptop

- We strongly recommend you to bring a laptop where you have permission to install software
- If this is not possible, a cloud environment will be made available with material and software already installed

#### R and RStudio IDE

- Follow installation instructions [here](https://posit.co/download/rstudio-desktop/)
- If you have R installed, make sure you have at least R version 4.1.0

#### R Packages Installation

Open RStudio and install the required R Packages:

```r
# Alternative: use pak::pak(...), see https://pak.r-lib.org/
install.packages(c(
  "tidyverse",
  "devtools",
  "duckplyr",
  "RMariaDB",
  "adbi",
  "dm",
  "pixarfilms",
  "nycflights13",
  "config",
  "rstudioapi",
  "progress",
  "DiagrammeR",
  "DiagrammeRsvg",
  "arrow",
  "odbc",
  "parquetize"
))
```

#### Discord

Discord will be our communication tool for the workshop.

- Register for a Discord account [here](https://discord.com/register)
- Make sure your name match the one you used to register for the conference
- Add the the workshop you are enrolled in your "About Me"
- You'll receive an invite to join the Discord server closer to the conference and you'll be added to the workshop channel

#### Database setup

We will be using DuckDB for demonstration purposes, with selected exercises targeting a publicly accessible database server.


#### Test your setup

Run the following lines of code:

```r
library(DBI)
library(duckdb)
duckdb_con <- dbConnect(duckdb())

dbExecute(duckdb_con, "INSTALL httpfs")
dbExecute(duckdb_con, "LOAD httpfs")
dbExecute(duckdb_con, "INSTALL json")
dbExecute(duckdb_con, "LOAD json")

dbDisconnect(duckdb_con)
```

#### Data

**We invite you to bring your own data and/or databases** to experiment with techniques during the last session on your own data.

We will provide a backup for you in case you don't have any.

## Schedule

| Time          | Activity         |
| :------------ | :--------------- |
| 09:00 - 10:30 | Talking to the database |
| 10:30 - 11:00 | *Coffee break*   |
| 11:00 - 12:30 | Working with files |
| 12:30 - 13:30 | *Lunch break*    |
| 13:30 - 15:00 | Digging in deeper |
| 15:00 - 15:30 | *Coffee break*   |
| 15:30 - 17:00 | Exercises - Bring your own data |

## Instructor

[Kirill Müller](https://www.cynkra.com/about/) has been working on the boundary between data and computer science for more than 25 years. He has been awarded five R consortium projects to improve database connectivity and performance in R. Kirill is a core contributor to several tidyverse packages, including dplyr and tibble, and is currently working on duckplyr, the next iteration of dplyr that uses DuckDB as a backend. He holds a Ph.D. in Civil Engineering from ETH Zurich and is a founder and partner at cynkra.

-----

![](https://i.creativecommons.org/l/by/4.0/88x31.png) This work is
licensed under a [Creative Commons Attribution 4.0 International
License](https://creativecommons.org/licenses/by/4.0/).
Add materials for your workshop in this folder. You can then remove this README, and rename this folder if you prefer.
