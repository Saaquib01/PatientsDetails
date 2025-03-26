import os
import requests
import pyodbc
import pandas as pd
import logging
import azure.functions as func  # Azure Functions package

# Fetch API Key & DB Connection from Environment Variables (Set in Azure)
API_KEY = os.getenv("PRACTICEHUB_API_KEY")
DB_CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

# Base API URL (request 100 rows per server page)
BASE_API_URL = "https://inspiredhealth.neptune.practicehub.io/api/patients?page_size=100"
HEADERS = {
    "x-practicehub-key": API_KEY,
    "x-app-details": "mywonderfulapp=help@practicehub.io",
    "User-Agent": "mywonderfulapp=help@practicehub.io",
    "Content-Type": "application/json"
}

def fetch_patients_data():
    """
    Fetches all patient records from the API using pagination.
    """
    all_patients = []
    current_url = BASE_API_URL

    while current_url:
        try:
            response = requests.get(current_url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                all_patients.extend(data.get("data", []))
                current_url = data.get("links", {}).get("next")  # Get next page URL
            else:
                logging.error(f"API Error: {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")
            break

    return pd.DataFrame(all_patients)

def update_sql_database(df):
    """
    Inserts/updates patient data into Azure SQL Database.
    """
    if df.empty:
        logging.warning("No patient data retrieved. Skipping database update.")
        return

    # Establish connection to Azure SQL
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='patients' AND xtype='U')
        CREATE TABLE patients (
            ID INT PRIMARY KEY,
            Patient_Number NVARCHAR(50),
            First_Name NVARCHAR(100),
            Last_Name NVARCHAR(100),
            Email NVARCHAR(255),
            Phone NVARCHAR(50),
            Sex NVARCHAR(10),
            DOB DATE,
            Medical_Insurance NVARCHAR(255),
            Total_Balance FLOAT,
            Last_Appointment DATETIME,
            Next_Appointment DATETIME,
            Created_At DATETIME,
            Updated_At DATETIME,
            Address_Num NVARCHAR(50),
            Address_Str NVARCHAR(255)
        )
    """)

    # Insert or update patient data
    for _, row in df.iterrows():
        cursor.execute("""
            MERGE INTO patients AS target
            USING (SELECT ? AS ID) AS source
            ON target.ID = source.ID
            WHEN MATCHED THEN
                UPDATE SET 
                    Patient_Number = ?, First_Name = ?, Last_Name = ?, 
                    Email = ?, Phone = ?, Sex = ?, DOB = ?, Medical_Insurance = ?, 
                    Total_Balance = ?, Last_Appointment = ?, Next_Appointment = ?, 
                    Created_At = ?, Updated_At = ?, Address_Num = ?, Address_Str = ?
            WHEN NOT MATCHED THEN
                INSERT (ID, Patient_Number, First_Name, Last_Name, Email, Phone, Sex, DOB, 
                        Medical_Insurance, Total_Balance, Last_Appointment, Next_Appointment, 
                        Created_At, Updated_At, Address_Num, Address_Str)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            row["ID"], row["Patient_Number"], row["First_Name"], row["Last_Name"],
            row["Email"], row["Phone"], row["Sex"], row["DOB"], row["Medical_Insurance"],
            row["Total_Balance"], row["Last_Appointment"], row["Next_Appointment"],
            row["Created_At"], row["Updated_At"], row["Address_Num"], row["Address_Str"],
            row["ID"], row["Patient_Number"], row["First_Name"], row["Last_Name"],
            row["Email"], row["Phone"], row["Sex"], row["DOB"], row["Medical_Insurance"],
            row["Total_Balance"], row["Last_Appointment"], row["Next_Appointment"],
            row["Created_At"], row["Updated_At"], row["Address_Num"], row["Address_Str"]
        ))

    conn.commit()
    conn.close()
    logging.info("Database update completed successfully!")

def main(mytimer: func.TimerRequest) -> None:
    """
    Azure Function that runs on a schedule to fetch and update patient data.
    """
    logging.info("Azure Function triggered. Fetching patient data...")
    
    df = fetch_patients_data()
    logging.info(f"Retrieved {len(df)} patient records from API.")

    update_sql_database(df)
    logging.info("Azure Function execution completed successfully.")
