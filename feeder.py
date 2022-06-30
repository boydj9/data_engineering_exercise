#Author - Jacob Boyd
#Name - feeder.py
#Summary - sample feed for code interview

import psycopg2
import csv
import os
import hashlib
from pydantic import BaseModel, ValidationError, validator
from typing import List
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

    @validator('vin')#makes sure VIN has a value
    def may_not_be_empty(cls, v):
        if v == '' or v is None:
            raise ValueError('VIN cannot be an empty string or left empty')
        return v

    # @validator('*', pre=True)#checks for insert, select, and delete in any string objects to check for injection attakcs
    # def injectionError(cls,v):
    #     if isinstance(v,str) and v is not None:
    #         if ('select' in str.casefold(v)) or ('insert' in str.casefold(v)) or ('delete' in str.casefold(v)):
    #             raise ValueError('SQL KEYWORD DETECTED. Please check values')
    #     return v

    @validator('dealer_msrp')#makes sure MSRP/List Price is set/non-zero
    def may_not_be_0(cls, v):
        if v == 0 or v is None:
            raise ValueError('MSRP/List Price cannot be 0')
        return v

SECRETUSER = os.environ['DBUSER']#allows use of environment variables, protects data
SECRETPASS = os.environ['DBPASS']#allows use of environment variables, protects data
conn = psycopg2.connect(host='localhost', dbname = 'postgres', user=SECRETUSER, password=SECRETPASS)
cur = conn.cursor()
validRules = [1,2]#list of valid rulesets (dealer1 and dealer2)
curDate = datetime.now()

with open('errorlog.txt', 'w'):#clear error file at start
    pass

def builder(): #create table 
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)", ('dealer_data',))#check db to see if the table has already been created, otherwise will create the table
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

def checkRow(car): #checks db to see if any changes are needed for a given car
    hashVal = car.hash
    selectQuery = "SELECT hash FROM dealer_data WHERE vin = '" + car.vin + "';"
    #print("VIN: %s\nHash: %s" % (car.vin,hashVal))
    cur.execute(selectQuery)
    results = cur.fetchone()
    hashDB = results[0]
    if(hashVal != hashDB):#will update values iff hash values are different
        #print("VIN = " + car.vin + "\nDB Val = " + str(hashDB) + "\nLH Val = " + str(hashVal) + "\n")
        with open('updatelog.txt', 'a') as updateLog:
            updateLog.write("Hash difference detected for VIN: " + car.vin + ". UPDATED at " + curDate.strftime("%d/%m/%Y %H:%M:%S") + ".\n")
        updateRows(car, hashVal)

def hashCar(car):#custom hash using hashlib to ensure proper hashing using sha256
    hashedVal = hashlib.new('sha256')
    stMileage = bytes(str(car.mileage), 'utf-8')
    stMSRP = bytes(str(car.dealer_msrp), 'utf-8')
    stInvoice = bytes(str(car.dealer_invoice), 'utf-8')
    stDate = bytes(car.dealer_inventory_entry_date.strftime("%d/%m/%Y %H:%M:%S"), 'utf-8')
    stCert = bytes(str(car.dealer_certified), 'utf-8')
    hashedVal.update(car.dealership_id.encode())
    hashedVal.update(car.vin.encode())
    hashedVal.update(stMileage)
    hashedVal.update(stMSRP)
    hashedVal.update(stInvoice)
    hashedVal.update(stDate)
    hashedVal.update(stCert)
    return str(hashedVal.digest())

def updateRows(car, hashVal):
    updateSQL = "UPDATE dealer_data SET hash = %s, dealership_id = %s, mileage = %s, dealer_msrp = %s, dealer_invoice = %s, dealer_inventory_entry_date = %s, dealer_certified = %s, updated_at = NOW() WHERE vin = %s"
    cur.execute(updateSQL, (hashVal, car.dealership_id, car.mileage, car.dealer_msrp, car.dealer_invoice, car.dealer_inventory_entry_date, car.dealer_certified, car.vin))
    conn.commit()

def dbCheck(vin):#check to see if vehicle VIN already present in the DB
    selectQuery = "SELECT * FROM dealer_data WHERE vin = '" + vin + "';"
    #print(selectQuery)
    cur.execute(selectQuery)
    check = cur.fetchone()
    if(check is None):
        return False
    else:
        return True

def insertCar(car):#inserts a row into the table
    cur.execute("INSERT INTO dealer_data (hash,dealership_id ,vin ,mileage ,is_new ,stock_number,dealer_year ,dealer_make,dealer_model,dealer_trim,dealer_model_number,dealer_msrp ,dealer_invoice ,dealer_body,dealer_inventory_entry_date ,dealer_exterior_color_description,dealer_interior_color_description,dealer_exterior_color_code,dealer_interior_color_code,dealer_installed_option_codes,dealer_installed_option_descriptions ,dealer_additional_specs,dealer_doors,dealer_drive_type,updated_at ,dealer_images,dealer_certified) Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", (car.hash,car.dealership_id,car.vin,car.mileage,car.is_new,car.stock_number,car.dealer_year,car.dealer_make,car.dealer_model,car.dealer_trim,car.dealer_model_number,car.dealer_msrp,car.dealer_invoice,car.dealer_body,car.dealer_inventory_entry_date,car.dealer_exterior_color_description,car.dealer_interior_color_description,car.dealer_exterior_color_code,car.dealer_interior_color_code,car.dealer_installed_option_codes,car.dealer_installed_option_descriptions,car.dealer_additional_specs,car.dealer_doors,car.dealer_drive_type,car.updated_at,car.dealer_images,car.dealer_certified))
    conn.commit()

def readIN(path, ruleset):#read in the data from the CSV and convert to pydantic BaseModel object
    with open(path, 'r') as fileIn:#read in file from path passed into function
        with open('errorlog.txt', 'a') as errorFile:#starts writing to error file
            csvReader = csv.reader(fileIn)
            headers = next(csvReader)#gets headers from first row in csv
            for row in csvReader:
                mappedRow = {headers[i]: row[i] for i in range(len(headers))}#creates dictionary of each row to the headers, allowing for easier references to values instead of indexes
                if(ruleset == 1):#ruleset for dealer1(different values for headers)
                    try:
                        newCar = Car(hash = '',dealership_id = mappedRow["Dealer ID"], vin = mappedRow["VIN"], mileage = mappedRow["Miles"], is_new = False if(mappedRow["Type"] == 'Used') else False, stock_number = mappedRow["Stock"], dealer_year = mappedRow["Year"], dealer_make = mappedRow["Make"], dealer_model = mappedRow["Model"], dealer_trim = mappedRow["Trim"], dealer_model_number = mappedRow["ModelNumber"], dealer_msrp = mappedRow["MSRP"] if (mappedRow["Type"] == 'New') else mappedRow["List Price"], dealer_invoice = mappedRow["Invoice"] if (mappedRow["Type"] == 'New') else mappedRow["List Price"], dealer_body = mappedRow["Body"], dealer_inventory_entry_date = datetime.strptime((mappedRow["DateInStock"]), '%m/%d/%Y'), dealer_exterior_color_description = mappedRow["ExteriorColor"], dealer_interior_color_description = mappedRow["InteriorColor"], dealer_exterior_color_code = mappedRow["ExteriorColorCode"], dealer_interior_color_code = mappedRow["InteriorColorCode"], dealer_transmission_name = mappedRow["Transmission"], dealer_installed_option_codes = list(mappedRow["OptionCode"].split(',')), dealer_installed_option_descriptions = list(mappedRow["OptionDescription"].split(',')), dealer_additional_specs = mappedRow["AdditionalSpecs"], dealer_doors = '', dealer_drive_type = mappedRow["Drivetrain"], dealer_images = list(mappedRow["ImageList"].split(',')), dealer_certified = True if(mappedRow["Certified"] == "Yes") else False)
                        newCar.hash = hashCar(newCar)
                        if(dbCheck(newCar.vin) == False):#checks to see if the VIN is present in the DB, inserts if not
                            insertCar(newCar)
                        else:
                            checkRow(newCar)
                    except ValidationError as e:
                        errorFile.write("Could not convert VIN: " + mappedRow["VIN"] +" from " + path + " to object. See following for more:\n" + e.json())
                if(ruleset == 2):#ruleset for dealer2(different values for headers)
                    try:
                        newCar = Car(hash = '',dealership_id = mappedRow["DealerId"], vin = mappedRow["VIN"], mileage = mappedRow["Mileage"], is_new = True if(mappedRow["New/Used"] == 'N') else False, stock_number = mappedRow["Stock #"], dealer_year = mappedRow["Year"], dealer_make = mappedRow["Make"], dealer_model = mappedRow["Model"], dealer_trim = mappedRow["Trim"], dealer_model_number = mappedRow["Model Code"], dealer_msrp = mappedRow["MSRP"] if (mappedRow["New/Used"] == 'N') else '', dealer_invoice = mappedRow["Invoice"] if (mappedRow["New/Used"] == 'N') else '', dealer_body = '', dealer_inventory_entry_date = datetime.strptime((mappedRow["Inventory Date"]), '%m/%d/%Y'), dealer_exterior_color_description = mappedRow["Exterior Color"], dealer_interior_color_description = mappedRow["Interior Color"], dealer_exterior_color_code = mappedRow["Exterior Color Code"], dealer_interior_color_code = mappedRow["Interior Color Code"], dealer_transmission_name = mappedRow["Transmission"], dealer_installed_option_codes = list(mappedRow["Option Codes"].split(',')), dealer_installed_option_descriptions = list(mappedRow["Options"].split(',')), dealer_additional_specs = '', dealer_doors = '', dealer_drive_type = '', dealer_images = list(mappedRow["Photos"].split("|")), dealer_certified = False)
                        newCar.hash = hashCar(newCar)
                        if(dbCheck(newCar.vin) == False):#checks to see if the VIN is present in the DB, inserts if not
                            insertCar(newCar)
                        else:
                            checkRow(newCar)
                    except ValidationError as e:
                        errorFile.write("Could not convert VIN: " + mappedRow["VIN"] +" from " + path + " to object. See following for more:\n" + e.json())
                

builder()
readIN("feeds/provider1/dealership1.csv", 1)
readIN("feeds/provider2/dealership2.csv", 2)

cur.close()
conn.close()