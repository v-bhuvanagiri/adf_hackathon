import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import RealDictCursor
import os
# Load CSV file into a pandas DataFrame
csv_file_path = r'C:\Users\Rohit\Projects\walmart\sight\api\inventorydata.csv'  # Path to your CSV file
df = pd.read_csv(csv_file_path)

# df["Date"]=pd.to_datetime(df["Date"])
# df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')






# DB_URI = os.getenv("DB_URI")

# Database connection details
db_username = 'postgres'
db_password = '210282'
db_host = 'localhost'
db_port = '5432'
db_name = 'sight'
db_table_name = 'inventory'

# Create a SQLAlchemy engine
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# connection = psycopg2.connect(DB_URI)
# cursor = connection.cursor(cursor_factory=RealDictCursor)

# cursor.execute('''CREATE TABLE Store(
#     Invoice_ID VARCHAR(20) PRIMARY KEY,
#     Branch VARCHAR(1),
#     City VARCHAR(30),
#     Customer_Type VARCHAR(10),
#     Gender VARCHAR(7),
#     Product_Type VARCHAR(30),
#     Price FLOAT,
#     Quantity INT,
#     Tax FLOAT,
#     Total FLOAT,
#     Date DATE,
#     Time TIME,
#     Payment VARCHAR(20),
#     Cogs FLOAT,
#     Gross_mp FLOAT,
#     Gross_inc FLOAT,
#     Rating FLOAT
#     )''')



# Push the DataFrame to the PostgreSQL table
df.to_sql(db_table_name, engine, if_exists='replace', index=False)

print(f"Data has been successfully pushed to the '{db_table_name}' table in the PostgreSQL database.")
