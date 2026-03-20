# Accessing Microstructure Data via APIs

## Overview

This repository provides a structured workflow to access **TAQ (Trade and Quote) data** from **WRDS**.
Similarly, it further provides a structured workflow to access **LSEG Tick History data**.
The goal is to ensure reproducibility, scalability, and clarity in handling high-frequency market microstructure data. 

## Requirement
TAQ data subscription in WRDS: https://wrds-www.wharton.upenn.edu/pages/about/data-vendors/nyse-trade-and-quote-taq/

Data subscription in LSEG Tick History: https://select.datascope.refinitiv.com/DataScope/

## Helpful Links
TidyFinance is a great resources for financial data science: https://www.tidy-finance.org/

## TAQ

### TAQ Data
TAQ is the one of the main resources for U.S. equity tick data.
You can access the underlying tick data or a daily aggregation called intraday indicators.
To read more about Intraday Indicators: https://wrds-www.wharton.upenn.edu/pages/grid-items/wrds-intraday-indicators/

When using the underlying tick data, procedures to clean this data have been written and commonly applied using Holden and Jacobsen (2014) which make a SAS code to clean available. Here is the link to the paper: https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12127
Further, Battalio et al (2025) notice an out-of-sequence problem with the SIP that releases the NBBO and propose a solution. Here is the link to the paper: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5907665

In the weeks ahead, I will release an R version of the code.

### TAQ-CRSP Link
The TAQ-CRSP link maps TAQ sym_root to CRSP permno. You can read more about it: https://wrds-www.wharton.upenn.edu/pages/wrds-research/database-linking-matrix/linking-taq-with-crsp/.

