#-------------------------------------------------------------------------------
# START ------------------------------------------------------------------------
#-------------------------------------------------------------------------------

# FUNCTION ---------------------------------------------------------------------
download.crsp <- function(con, 
                          
                          start_date, 
                          
                          end_date,
                          
                          instrument_list = c(NULL),
                          
                          batch_size = 500){
  
  # DAILY CRSP
  dsf_db <- tbl(wrds, I("crsp.dsf_v2"))
  
  stksecurityinfohist_db <- tbl(wrds, I("crsp.stksecurityinfohist"))
  
  # FROM CRSP
  permnos_tbl <- stksecurityinfohist_db |>
    distinct(permno)
  
  # TO FILTER FOR ONLY STOCKS OF INTEREST FROM E.G. USE TAQ TO GET PERMNO
  if (length(instrument_list) > 0) {
    permnos_tbl <- permnos_tbl |>
      filter(permno %in% instrument_list)
  }
  
  # COPY AND PASTE FROM https://www.tidy-finance.org/r/wrds-crsp-and-compustat.html
  permnos <- permnos_tbl |>
    pull(permno)
  
  batches <- ceiling(length(permnos) / batch_size)
  
  for (j in 1:batches) {
    
    permno_batch <- permnos[
      ((j - 1) * batch_size + 1):min(j * batch_size, length(permnos))
    ]
    
    crsp_daily_sub <- dsf_db |>
      filter(permno %in% permno_batch) |> 
      # SELECTION DATE
      filter(dlycaldt >= start_date & dlycaldt <= end_date) |> 
      inner_join(
        stksecurityinfohist_db |>
                    # no special share types (sharetype = 'NS')
          filter(sharetype == "NS" & 
                   # security type equity (securitytype = 'EQTY')
                   securitytype == "EQTY" & 
                   # security sub type common stock (securitysubtype = 'COM'), 
                   securitysubtype == "COM" & 
                   # only US-listed stocks
                   usincflg == "Y" & 
                   # issuers that are a corporation (issuertype %in% c("ACOR", "CORP"))
                   issuertype %in% c("ACOR", "CORP") & 
                   # we use only stock prices from NYSE, Amex, and NASDAQ (primaryexch %in% c("N", "A", "Q")) 
                   primaryexch %in% c("N", "A", "Q") &
                   # when or after issuance (conditionaltype %in% c("RW", "NW")) 
                   conditionaltype %in% c("RW", "NW") &
                   # for actively traded stocks (tradingstatusflg == "A")
                   tradingstatusflg == "A") |> 
          select(permno, secinfostartdt, secinfoenddt),
        join_by(permno)
      ) |>
      # (iv) we keep only months within permno-specific start dates (secinfostartdt) and end dates (secinfoenddt)
      filter(dlycaldt >= secinfostartdt & dlycaldt <= secinfoenddt)  |> 
      mutate(
        mktcap = (1000 * shrout / dlycumfacshr) * dlyprc / 10^6,
        mktcap = na_if(mktcap, 0),
        dlyprc = dlyprc/ dlycumfacpr
      ) |>
      collect() |>
      drop_na()
    
    if (nrow(crsp_daily_sub) > 0) {
      
      dbWriteTable(
        con,
        "crsp_daily",
        value = crsp_daily_sub,
        overwrite = ifelse(j == 1, TRUE, FALSE),
        append = ifelse(j != 1, TRUE, FALSE)
      )
    }
    
    message("Batch ", j, " out of ", batches, " done (", percent(j / batches), ")\n")
  }
  
}

# LOAD PACKAGES
library(RPostgres)
library(tidyverse)
library(tidyfinance)
library(duckdb)
library(arrow)

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

# ASSUMING YOU HAVE ALREADY SET THE CREDENTIALS set_wrds_credentials is commented out
# set_wrds_credentials()

wrds <- get_wrds_connection()

# PARAMETERS: END OF PREVIOUS YEAR
crsp_sample_date <- ymd("2024-12-31")

# CREATE EMPTY DATA BASE
con <- dbConnect(duckdb(), dbdir = paste0("./crsp_sample.duckdb"))

# DOWNLOAD ALL US COMMON EQUITY
download.crsp(con = con, 
              start_date = crsp_sample_date,
              end_date = crsp_sample_date)

crsp_sample <- dbGetQuery(con, "SELECT permno FROM crsp_daily")

taq_crsp_link <- tbl(wrds, I("wrdsapps_link_crsp_taqm.taqmclink")) |> 
  distinct(sym_root, sym_suffix, permno) |> 
  select(sym_root, sym_suffix, permno) |> 
  collect()

taq_sample <- crsp_sample |> 
  left_join(taq_crsp_link, by = "permno") |>
  filter(is.na(sym_suffix) == TRUE) |> 
  collect()

# DISCONNECT TO DUCKDB DATABASE
dbDisconnect(con)

# DISCONNECT TO WRDS
dbDisconnect(wrds)

#-------------------------------------------------------------------------------
# END --------------------------------------------------------------------------
#-------------------------------------------------------------------------------