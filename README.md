Jacob Boyd
Data Engineering Exercise

Contains the infrastructure and code for a feed ingestion system using Python, Postgres, and CSVs. Feeder.py contains the code, which pulls data from the CSVs stored in the folder structures. 

Used tools/libraries:
postgres,psycopg2,csv, os, hashlib, pydantic, datetime, typing

To run the code, download the repository from github either through downloading the zip or enter the command line argument in the desired location: "git clone https://github.com/boydj9/data_engineering_exercise.git"
Next in command prompt, run "pip build jacobdataeexercise" to install the required dependencies
After the dependencies are installed, get the postgres server up and running using "postgres" (recommended to run in another cmd window)
after the postgres server is running, using the command "python feeder.py" will run the feed








No certified column in dealership 2, defaulting to False
no list price in dealership 2, defaulting to empty string

Break down into 2 rulesets, allow for expansion in future
use header row to convert the list into a dictionary, easier mapping

Current Flow: 
Imports
DB Connection and global variables
Build table if not built
read in one at a time row and convert to BaseModel
check table to see if VIN is already present
    If VIN present then no insert
    If hash values different then update the hash-used values
        update row if any differences
close connections

hash values
-decided to only use 7 attributes for the hash values, as most of the aspects of a car will not change nor should be changed
-vin, dealership_id, mileage, dealer_msrp, dealer_invoice, dealer_inventory_entry_date, dealer_certified
    -vin should always be the same, but as unique car identifer useful for determining values of hash
    -dealership_id may change if a car gets moved from one dealership to another
    -mileage may vary
    -dealer_msrp/dealer_invoice could in theory change if prices change (i.e. list price)
    -dealer_inventory_entry_date is used more as an identifier and shouldn't change unless moved between inventories
    -dealer_certified could change in theory, not exactly sure what it represents
these values get hashed to use as a unique identifier for these values, and will be checked to see if any of these values need to be updated
    
Future: 
able to easily add more rulesets
add additional validations such as sql injection checks
modify logging systems for more info
