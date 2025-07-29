import psycopg2
from dotenv import load_dotenv
import os
load_dotenv(override=True)

# Replace with your actual credentials
conn_params = {
    'host': 'postgresdb.dev-ownvda.aws.telekom.de', 
    'port': '5432', 
    'dbname': 'ownvda-dev', 
    'user': 'user-evaluation', 
    'password': 'a7K9$p!zR#tG3vW@sL2mF*qH'
    }

try:
    # Establish the connection
    conn = psycopg2.connect(**conn_params)
    print("✅ Connected to PostgreSQL database!")

    # Create a cursor to execute queries
    cur = conn.cursor()
    
    # Example query
        
    version = cur.fetchone()
    print("PostgreSQL version:", version)

    # Close cursor and connection
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("❌ Error connecting to PostgreSQL:", e)