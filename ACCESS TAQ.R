#-------------------------------------------------------------------------------
# START ------------------------------------------------------------------------
#-------------------------------------------------------------------------------

# LOAD PACKAGES
library(tidyverse)
library(RPostgres)
library(tidyfinance)

user <- "USERNAME"
password <- "PASSWORD"

# CREATE CONNECTION
wrds <- dbConnect(
  Postgres(),
  host = "wrds-pgdata.wharton.upenn.edu",
  dbname = "wrds",
  port = 9737,
  sslmode = "require",
  user = user,
  password = password
)

set_wrds_credentials()

wrds <- get_wrds_connection()

# PARAMETERS
taq_start_date <- "2025-11-01"
taq_end_date <- "2025-11-31"

year_wrds_iid <- year(taq_start_date)

# GET INTRADAY INDICATORS
taq_data <- tbl(wrds, I(paste0("taqmsec.wrds_iid_", year_wrds_iid)))

# GET TICK DATA 
# NBBO
DailyNBBO <- tbl(wrds, I(paste0("taqm_", year_wrds_iid,".nbbom_", year_wrds_iid))) |>
  filter(date >= taq_start_date & date <= taq_end_date) 

# QUOTES
DailyQuote <- tbl(wrds, I(paste0("taqm_", year_wrds_iid, ".cqm_", year_wrds_iid))) |>
  filter(date >= taq_start_date & date <= taq_end_date) 

# TRADES
DailyTrade <- tbl(wrds, I(paste0("taqm_", year_wrds_iid, ".ctm_", year_wrds_iid))) |>
  filter(date >= taq_start_date & date <= taq_end_date) 

# DISCONNECT
dbDisconnect(wrds)

#-------------------------------------------------------------------------------
# END --------------------------------------------------------------------------
#-------------------------------------------------------------------------------
