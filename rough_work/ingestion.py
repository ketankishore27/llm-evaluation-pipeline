import sqlite3
import pandas as pd

# Load the CSV file
csv_file = "data/sampleFile.csv"  # Replace with your actual file path
df = pd.read_csv(csv_file)

# Connect to SQLite database (or create it)
conn = sqlite3.connect('conversations.db')
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS conversations (
        sender_id TEXT,
        conversation_time TEXT,
        type_name TEXT,
        text TEXT,
        buttons_feedback TEXT
    )
''')

# Insert data into the table
df.to_sql('conversations', conn, if_exists='append', index=False)

# Commit and close
conn.commit()
conn.close()

print("Data inserted successfully.")
