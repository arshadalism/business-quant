import csv
import mysql.connector as sqltor
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()


def insert_data_from_csv(filename, tablename, host, user, passwd, database):
    try:
        mycon = sqltor.connect(host=host, user=user, passwd=passwd, database=database)
        print("Connected to the database successfully.")
    except sqltor.Error as e:
        print(f"Error connecting to database: {e}")
        return

    if mycon.is_connected():
        cursor = mycon.cursor()

        # Create the table if it doesn't exist
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {tablename} (ticker VARCHAR(255), date DATE, revenue BIGINT, gp BIGINT, fcf BIGINT, capex BIGINT)")

        # Insert data from CSV
        with open(filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header row
            for row in csv_reader:
                # Convert date format from 'MM/DD/YYYY' to 'YYYY-MM-DD'
                if row[1]:  # Check if the date field is not empty
                    date_object = datetime.strptime(row[1], "%m/%d/%Y").date()
                    row[1] = date_object.strftime("%Y-%m-%d")

                # Convert empty strings to None
                row = [None if value == '' else value for value in row]

                cursor.execute(
                    f"INSERT INTO {tablename} (ticker, date, revenue, gp, fcf, capex) VALUES (%s, %s, %s, %s, %s, %s)",
                    row)

        mycon.commit()
        cursor.close()
        mycon.close()
        print("Data inserted successfully.")


if __name__ == '__main__':
    # Provide the necessary database connection details and CSV file name
    host = "localhost"
    user = "root"
    passwd = os.environ.get('PASSWORD')
    database = "financial_data"
    csv_filename = "Sample-Data-Historic.csv"
    table_name = "business_data"

    # Call the function to insert data from the CSV file into the database table
    insert_data_from_csv(csv_filename, table_name, host, user, passwd, database)
