# barchartacs - create volatility skew analysis for commodity contracts using commodity option settlement data from Barchart's Adavanced Commodity Service (ACS)

### Overview:
Barchart.com has a paid service that - for a monthly fee - allows users to obtain daily option price settlements on all major commodities.  This project uses selscrape selenium python api to login to and obtain these files, and to update a Postgres database (sec_db) which contains both options and futures daily settlements.  After creating the Postgres database, several csv files are created which house implied volatilties for these options, and volatility skew information for all OTM options, normalized by percentage out of the money.

### Usage: Building the Postgres Database: 
#### First, you must create/initialize a local (or remote) Postgres database:
On Linux (Windows not supported), run the .sh ```step_00_create_tables.sh```

#### Second, there are two different ways of adding options and futures data to the postgres database:

* Method 1 - Run the ipynb notebooks
    * ```step_01_download_monthly_acs_files.ipynb```, 
    * ```step_02_options_table_loader.ipynb```, 
    * ```step_03_underlying_table_loader.ipynb```, 
    * ```step_04_build_df_iv_skew_csvs.ipynb```, and 
    * ```step_05_options_table_daily_loader.ipynb``` .


* Method 2 - On Linux (Windows not supported), run the .sh: 
    * ```step_01_download_monthly_acs_files.sh```, 
    * ```step_02_options_table_loader.sh```, 
    * ```step_03_underlying_table_loader.sh```, 
    * then run the ```step_04_build_df_iv_skew_csvs.ipynb``` and ```step_05_options_table_daily_loader.ipynb``` .
    
    