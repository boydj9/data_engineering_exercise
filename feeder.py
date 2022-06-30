import psycopg2
import csv
import os
from pydantic import BaseModel, ValidationError, validator
from typing import List, Optional
from datetime import datetime

class Car(BaseModel):
    hash : str
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

    @validator('vin')
    def may_not_be_empty(cls, v):
        if v == '':
            raise ValueError('cannot be an empty string')
        return v.title()

SECRETUSER = os.environ['DBUSER']
SECRETPASS = os.environ['DBPASS']
conn = psycopg2.connect(host='localhost', dbname = 'postgres', user=SECRETUSER, password=SECRETPASS)
cur = conn.cursor()
validRules = [1,2]
curDate = datetime.now()

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

def dbCheck(vin):
    selectQuery = "SELECT * FROM dealer_data WHERE vin = '" + vin + "';"
    #print(selectQuery)
    cur.execute(selectQuery)
    check = cur.fetchone()
    if(check is None):
        return False
    else:
        return True

def insertCar(car):
    cur.execute("INSERT INTO dealer_data (hash,dealership_id ,vin ,mileage ,is_new ,stock_number,dealer_year ,dealer_make,dealer_model,dealer_trim,dealer_model_number,dealer_msrp ,dealer_invoice ,dealer_body,dealer_inventory_entry_date ,dealer_exterior_color_description,dealer_interior_color_description,dealer_exterior_color_code,dealer_interior_color_code,dealer_installed_option_codes,dealer_installed_option_descriptions ,dealer_additional_specs,dealer_doors,dealer_drive_type,updated_at ,dealer_images,dealer_certified) Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", (car.hash,car.dealership_id,car.vin,car.mileage,car.is_new,car.stock_number,car.dealer_year,car.dealer_make,car.dealer_model,car.dealer_trim,car.dealer_model_number,car.dealer_msrp,car.dealer_invoice,car.dealer_body,car.dealer_inventory_entry_date,car.dealer_exterior_color_description,car.dealer_interior_color_description,car.dealer_exterior_color_code,car.dealer_interior_color_code,car.dealer_installed_option_codes,car.dealer_installed_option_descriptions,car.dealer_additional_specs,car.dealer_doors,car.dealer_drive_type,car.updated_at,car.dealer_images,car.dealer_certified))
    conn.commit()

def readIN(path, ruleset):
    with open(path, 'r') as fileIn:
        with open('errorlog.txt', 'a') as errorFile:
            csvReader = csv.reader(fileIn)
            headers = next(csvReader)
            for row in csvReader:
                mappedRow = {headers[i]: row[i] for i in range(len(headers))}
                if(ruleset == 1):
                    try:
                        newCar = Car(hash = mappedRow["VIN"]+curDate.strftime("%m/%d/%Y"),dealership_id = mappedRow["Dealer ID"], vin = mappedRow["VIN"], mileage = mappedRow["Miles"], is_new = False if(mappedRow["Type"] == 'Used') else False, stock_number = mappedRow["Stock"], dealer_year = mappedRow["Year"], dealer_make = mappedRow["Make"], dealer_model = mappedRow["Model"], dealer_trim = mappedRow["Trim"], dealer_model_number = mappedRow["ModelNumber"], dealer_msrp = mappedRow["MSRP"] if (mappedRow["Type"] == 'New') else mappedRow["List Price"], dealer_invoice = mappedRow["Invoice"] if (mappedRow["Type"] == 'New') else mappedRow["List Price"], dealer_body = mappedRow["Body"], dealer_inventory_entry_date = datetime.strptime((mappedRow["DateInStock"]), '%m/%d/%Y'), dealer_exterior_color_description = mappedRow["ExteriorColor"], dealer_interior_color_description = mappedRow["InteriorColor"], dealer_exterior_color_code = mappedRow["ExteriorColorCode"], dealer_interior_color_code = mappedRow["InteriorColorCode"], dealer_transmission_name = mappedRow["Transmission"], dealer_installed_option_codes = list(mappedRow["OptionCode"].split(',')), dealer_installed_option_descriptions = list(mappedRow["OptionDescription"].split(',')), dealer_additional_specs = mappedRow["AdditionalSpecs"], dealer_doors = '', dealer_drive_type = mappedRow["Drivetrain"], dealer_images = list(mappedRow["ImageList"].split(',')), dealer_certified = True if(mappedRow["Certified"] == "Yes") else False)
                        if(dbCheck(newCar.vin) == False):
                            insertCar(newCar)
                    except ValidationError as e:
                        errorFile.write("Could not convert VIN: " + mappedRow["VIN"] +" from " + path + " to object. See following for more:\n" + e.json())
                if(ruleset == 2):
                    try:
                        newCar = Car(hash = mappedRow["VIN"]+curDate.strftime("%m/%d/%Y"),dealership_id = mappedRow["DealerId"], vin = mappedRow["VIN"], mileage = mappedRow["Mileage"], is_new = True if(mappedRow["New/Used"] == 'N') else False, stock_number = mappedRow["Stock #"], dealer_year = mappedRow["Year"], dealer_make = mappedRow["Make"], dealer_model = mappedRow["Model"], dealer_trim = mappedRow["Trim"], dealer_model_number = mappedRow["Model Code"], dealer_msrp = mappedRow["MSRP"] if (mappedRow["New/Used"] == 'N') else '', dealer_invoice = mappedRow["Invoice"] if (mappedRow["New/Used"] == 'N') else '', dealer_body = '', dealer_inventory_entry_date = datetime.strptime((mappedRow["Inventory Date"]), '%m/%d/%Y'), dealer_exterior_color_description = mappedRow["Exterior Color"], dealer_interior_color_description = mappedRow["Interior Color"], dealer_exterior_color_code = mappedRow["Exterior Color Code"], dealer_interior_color_code = mappedRow["Interior Color Code"], dealer_transmission_name = mappedRow["Transmission"], dealer_installed_option_codes = list(mappedRow["Option Codes"].split(',')), dealer_installed_option_descriptions = list(mappedRow["Options"].split(',')), dealer_additional_specs = '', dealer_doors = '', dealer_drive_type = '', dealer_images = list(mappedRow["Photos"].split("|")), dealer_certified = True)
                        if(dbCheck(newCar.vin) == False):
                            insertCar(newCar)

                    except ValidationError as e:
                        errorFile.write("Could not convert VIN: " + mappedRow["VIN"] +" from " + path + " to object. See following for more:\n" + e.json())
                
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

cur.close()
conn.close()