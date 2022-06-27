from cmath import e
import psycopg2
import csv

conn = psycopg2.connect(host='localhost', dbname = 'postgres', user='postgres', password='root')
cur = conn.cursor()

builder()
readIN("provider1/dealership1.csv", 1)
readIN("provider2/dealership2.csv", 2)


def builder(): #create table 
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)", ('dealer_data',))
    check = cur.fetchone()
    if check[0] == False:
        cur.execute("""
            CREATE TABLE dealer_data (
            hash text PRIMARY KEY,
            dealership_id text NOT NULL,
            vin text NOT NULL,
            mileage integer,
            is_new boolean,
            stock_number text,
            dealer_year integer,
            dealer_make text,
            dealer_model text,
            dealer_trim text,
            dealer_model_number text,
            dealer_msrp integer,
            dealer_invoice integer,
            dealer_body text,
            dealer_inventory_entry_date date,
            dealer_exterior_color_description text,
            dealer_interior_color_description text,
            dealer_exterior_color_code text,
            dealer_interior_color_code text,
            dealer_transmission_name text,
            dealer_installed_option_codes text[],
            dealer_installed_option_descriptions text[],
            dealer_additional_specs text,
            dealer_doors text,
            dealer_drive_type text,
            updated_at timestamp with time zone NOT NULL DEFAULT now(),
            dealer_images text[],
            dealer_certified boolean
        );
        """)
        conn.commit()

def readIN(path, ruleset):
    with open(path, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if checkRow(row, ruleset) == "insert":
                cur.execute(
                    "INSERT INTO accounts VALUES (%s,%s)",
                    row
                )
                conn.commit()
            elif checkRow(row, ruleset) == "edit":
                #add options here
    
def checkRow(values,ruleset):
    #add rules in here to check, remember rulesets for different dealers