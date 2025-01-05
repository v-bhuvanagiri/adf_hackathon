import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Load Excel file into a pandas DataFrame (only the specific sheet for current_accounts)
excel_file_path = r'C:\Users\Vaishnavi\Downloads\dummy_data.xlsx'  # Replace with the path to your Excel file
df = pd.read_excel(excel_file_path, sheet_name='current_accounts')  # Load the 'current_accounts' sheet

# Ensure datetime columns are parsed correctly
df["close_dt"] = pd.to_datetime(df["close_dt"], errors='coerce').dt.date
df["charge_off_date"] = pd.to_datetime(df["charge_off_date"], errors='coerce').dt.date

# Database connection details
db_username = 'postgres'
db_password = '1629'
db_host = 'localhost'
db_port = '5432'
db_name = 'adf'
db_table_name = 'current_accounts'

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    database=db_name,
    user=db_username,
    password=db_password,
    host=db_host,
    port=db_port
)

cursor = connection.cursor()

# Create the table
cursor.execute(f'''CREATE TABLE IF NOT EXISTS {db_table_name} (
    id VARCHAR(255),
    offer_type INT,
    current_dpd INT,
    close_dt DATE,
    charge_off_date DATE,
    gross_co_bal DECIMAL,
    prin_co_bal DECIMAL,
    curr_status VARCHAR(255),
    curr_pymt_type VARCHAR(255),
    curr_prin_bal DECIMAL
    )''')

# Commit the table creation
connection.commit()

# Create a SQLAlchemy engine for pushing data
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Push the DataFrame to the PostgreSQL table
df.to_sql(db_table_name, engine, if_exists='replace', index=False)

# Close the database connection
cursor.close()
connection.close()

print(f"Data from the 'current_accounts' sheet has been successfully pushed to the '{db_table_name}' table in the PostgreSQL database.")
