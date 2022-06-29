import psycopg2
import csv
import os
from pydantic import BaseModel, ValidationError
from typing import List, Optional
from datetime import datetime

class Car(BaseModel):
    hash = ""
    dealership_id : str
    vin : str
    mileage : int
    is_new : bool
    stock_number  : str
    dealer_year : int
    dealer_make  : str
    dealer_model  : str
    dealer_trim  : str
    dealer_model_number  : str
    dealer_msrp : int
    dealer_invoice : int
    dealer_body  : str
    dealer_inventory_entry_date : datetime
    dealer_exterior_color_description  : str
    dealer_interior_color_description  : str
    dealer_exterior_color_code  : str
    dealer_interior_color_code  : str
    dealer_installed_option_codes  : List[str]
    dealer_installed_option_descriptions : List[str]
    dealer_additional_specs  : str
    dealer_doors  = ''
    dealer_drive_type  : str
    updated_at = datetime.now()
    dealer_images  : List[str]
    dealer_certified : bool

SECRETUSER = os.environ['DBUSER']
SECRETPASS = os.environ['DBPASS']
conn = psycopg2.connect(host='localhost', dbname = 'postgres', user=SECRETUSER, password=SECRETPASS)
cur = conn.cursor()
validRules = [1,2]
ruleset1 = []
ruleset2 = []

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

def checkRow(row,ruleset):
    if(ruleset not in validRules):
        print("%d is not a valid Ruleset", int(ruleset))
        return "INVALID"
    
    elif(ruleset == 1):
        NULLVALS = []
        for x in range(0,len(row)):
            if(x == 0)&(not row[x]):
                continue


def readIN(path, ruleset):
    with open(path, 'r') as f:
        csvReader = csv.reader(f)
        headers = next(csvReader)
        for row in csvReader:
            mappedRow = {headers[i]: row[i] for i in range(len(headers))}
            if(ruleset == 1):
                try:
                    newCar = Car(dealership_id = mappedRow["Dealer ID"], vin = mappedRow["VIN"], mileage = mappedRow["Miles"], is_new = False if(mappedRow["Type"] == 'Used') else False, stock_number = mappedRow["Stock"], dealer_year = mappedRow["Year"], dealer_make = mappedRow["Make"], dealer_model = mappedRow["Model"], dealer_trim = mappedRow["Trim"], dealer_model_number = mappedRow["ModelNumber"], dealer_msrp = mappedRow["MSRP"] if (mappedRow["Type"] == 'New') else mappedRow["List Price"], dealer_invoice = mappedRow["Invoice"] if (mappedRow["Type"] == 'New') else mappedRow["List Price"], dealer_body = mappedRow["Body"], dealer_inventory_entry_date = datetime.strptime((mappedRow["DateInStock"]), '%m/%d/%Y'), dealer_exterior_color_description = mappedRow["ExteriorColor"], dealer_interior_color_description = mappedRow["InteriorColor"], dealer_exterior_color_code = mappedRow["ExteriorColorCode"], dealer_interior_color_code = mappedRow["InteriorColorCode"], dealer_transmission_name = mappedRow["Transmission"], dealer_installed_option_codes = list(map(str,mappedRow["OptionCode"])), dealer_installed_option_descriptions = list(map(str,mappedRow["OptionDescription"])), dealer_additional_specs = mappedRow["AdditionalSpecs"], dealer_doors = '', dealer_drive_type = mappedRow["Drivetrain"], dealer_images = list(mappedRow["ImageList"].split()), dealer_certified = mappedRow["Certified"])
                    print("%s\n" %newCar)
                except ValidationError as e:
                    print(e.json())
            
            # if checkRow(row, ruleset) == "INSERT":
            #     cur.execute(
            #         "INSERT INTO accounts VALUES (%s,%s)",
            #         row
            #     )
            #     conn.commit()
            # elif checkRow(row, ruleset) == "UPDATE":
            #     #add options here

builder()
readIN("feeds/provider1/dealership1.csv", 1)
readIN("feeds/provider2/dealership2.csv", 2)