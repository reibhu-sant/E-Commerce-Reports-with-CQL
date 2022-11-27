from database import *
import csv
from datetime import datetime

if __name__ == "__main__":
    try:
        # Creating session object
        session = create_session()

        # Keyspace to be used for performing various operations
        KEYSPACE = 'e_commerce'

        # Setting Keyspace in the cassandra session
        # Code for setting session with cassandra database
        set_session_keyspace(session, KEYSPACE)
        # Creating table 'Customer'
        create_customer_table_query = f""" CREATE TABLE IF NOT EXISTS Customer (
        cust_id text,
        first_name text,
        last_name text,
        registered_on timestamp,
        PRIMARY KEY (cust_id));"""

        if execute_query(session, create_customer_table_query) is not None:
            print("Customer table is successfully created.")

        # Creating table 'Product'
        create_product_table_query = f""" CREATE TABLE IF NOT EXISTS Product (
        prdt_id text,
        title text,
        PRIMARY KEY (prdt_id));"""

        if execute_query(session, create_product_table_query) is not None:
            print("Product table is successfully created.")

        # Creating table 'Product_Liked_By_Customer'
        create_product_liked_by_customer_table_query = f""" CREATE TABLE IF NOT EXISTS Product_Liked_By_Customer(
        cust_id text,
        first_name text,
        last_name text,
        liked_prdt_id text,
        liked_on timestamp,
        title text,
        PRIMARY KEY (cust_id,liked_prdt_id,liked_on));"""

        if execute_query(session, create_product_liked_by_customer_table_query) is not None:
            print("Product_Liked_By_Customer table is successfully created.")

        # Inserting Customer data in the table
        customer_data_insert_query = "INSERT INTO Customer(cust_id, first_name, last_name ,registered_on) Values('%s','%s','%s','%d')"
        with open("config/customers.csv", "r") as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                execute_query(session, customer_data_insert_query % (
                    row[0],
                    row[1],
                    row[2],
                    int(float(datetime.now().strftime("%f"))) * 1000))

        # Inserting Product data in the table
        product_data_insert_query = "INSERT INTO Product(prdt_id,title) Values('%s','%s')"
        with open("config/products.csv", "r") as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                execute_query(session, product_data_insert_query % (
                    row[0],
                    row[1]))

        # Inserting Product_Liked_By_Customer data in the table
        product_liked_by_customer_data_insert_query = "INSERT INTO Product_Liked_By_Customer(cust_id, first_name, last_name, liked_prdt_id, liked_on, title) Values('%s','%s','%s','%s','%d','%s')"
        with open("config/product_liked_by_customer.csv", "r") as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                execute_query(session, product_liked_by_customer_data_insert_query % (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    int(float(datetime.now().strftime("%f"))) * 1000,
                    row[4]))

    except Exception as e:
        print("Error in the execution : ", str(e))

