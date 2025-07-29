import pandas as pd
import asyncio
from utils.llm_operations_langchain import prompt_exm, parser, create_batch_yaml_sample
import json
import sqlite3
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import os
import sqlite3
load_dotenv(override=True)


def get_data_postgres():

    """
    Retrieves conversation data from the remote PostgreSQL database, 
    filtered by conversation time of the current and previous day.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.
    """

    print("*" * 8, "Get-Data Server")
    try:
        conversation_schema=os.getenv("conversation_db_schema", None)
        conversation_table_name=os.getenv("conversation_table_name", None)
        conn_params_conversation = {
                'host': os.getenv("hostName_dev", None), 
                'port': '5432', 
                'dbname': os.getenv("conversation_db_name", None), 
                'user': os.getenv("user_name", None), 
                'password': os.getenv("password", None)
        }
        current_date = pd.to_datetime('2023-06-25').date() #pd.Timestamp.now().date()
        prev_date = current_date - pd.Timedelta(days=1)

        engine = create_engine(f"postgresql+psycopg2://{conn_params_conversation['user']}:{quote_plus(conn_params_conversation['password'])}@{conn_params_conversation['host']}:{conn_params_conversation['port']}/{conn_params_conversation['dbname']}")
        data = pd.read_sql(f"SELECT * FROM {conversation_schema}.{conversation_table_name}", engine)
        data["conversation time"] = data["conversation time"].apply(lambda x: pd.to_datetime(x))
        data["conversation_date"] = data["conversation time"].apply(lambda x: x.date())
        data_sample = data[(data["conversation_date"] > prev_date) & (data["conversation_date"] <= current_date)]
        data_sample = data_sample.rename(columns = {"conversation time": "conversation_time"})
        engine.dispose()
        return data_sample

    except Exception as e:
        print(e)

def get_data_local():

    print("*" * 8, "Get-Data Local")
    conn = sqlite3.connect('conversations.db')
    data = pd.read_sql_query("SELECT * FROM conversations", conn)
    conn.close()
    data_sample = data[data['sender_id'].isin(['Ny7i23GjoezOA_h6NjwIK', 'btLyma2P7Yq2Owe9R5O17', 'yCCKo0asCCrhjWVvgCGQw'])]
    return data_sample


def get_data():

    """
    Retrieves conversation data based on run_config environment variable.

    If run_config is "server", data is retrieved from the remote PostgreSQL database.
    If run_config is "local", data is retrieved from a local SQLite database.

    Args:
        None
    
    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.
    
    Raises:
        "Try Catch Exception: No Valid run_config found while extracting data"
    """

    if os.getenv("run_config", None) == "server":
        return get_data_postgres()

    elif os.getenv("run_config", None) == "local":
        return get_data_local()
    
    else:
        raise "Try Catch Exception: No Valid run_config found while extracting data"

def set_slot_processing(text):
    replaced_text = text.replace("/SetSlots(", "clicks `").replace("=", "` button with `").replace(")", "`")
    return replaced_text

async def process_chats(sender_id, df):

    data_sample = df[df['sender_id'] == sender_id]
    data_sample = data_sample.sort_values(by = ["conversation_time"])
    conversation_start_time = data_sample['conversation_time'].min()
    conversation_end_time = data_sample['conversation_time'].max()
    
    df_vals = data_sample[data_sample["type_name"].isin(["user", "bot"])][["type_name", "text"]].values
    conversation = ""
    for speaker, text in df_vals:
        text = text.strip("\n").strip()    
        if "/SetSlots" in text:
            text = set_slot_processing(text = text)
        conversation += f"{speaker}: {text}\n" 

    prompt_final = prompt_exm.format(chat_transcript = conversation, 
                        format_instruction=parser.get_format_instructions())
    
    batch_request_sample = create_batch_yaml_sample(prompt = prompt_final, sender_id = sender_id)

    return batch_request_sample, conversation_start_time, conversation_end_time

def create_batch_file(results):

    print("*" * 8, "Create BatchFile")
    with open("messages.jsonl", "w") as f:
        for batch_request_json, _, _ in results:
            json.dump(batch_request_json, f)
            f.write("\n")

async def process_all_senders(data):

    print("*" * 8, "Process-Chats")
    unique_senders = data.sender_id.unique()
    tasks = [asyncio.create_task(process_chats(sender, data)) for sender in unique_senders]
    results = await asyncio.gather(*tasks)
    return results

def update_batch_postgres(batch_response, cursor):
    print("*" * 8, "Update BatchInfo in csv")
    batch_id = batch_response.id
    status = batch_response.status
    created_at = batch_response.created_at
    schema_name = os.getenv("track_db_schema", None)
    table_name = os.getenv("track_table_name", None)
    sql_String = f"""INSERT INTO {schema_name}.{table_name} 
                    (batch_id, job_status, creation_time, tracking_reference) 
                    VALUES ('{batch_id}', '{status}', '{created_at}', 'TRACK');
                """
    
    cursor.execute(sql_String)
    cursor.connection.commit()
    return True

def update_batch_local(batch_response, cursor):

    print("*" * 8, "Update BatchInfo in csv")
    try:
        batch_id = batch_response.id
        status = batch_response.status
        created_at = batch_response.created_at
        table = "track_status"
        sql_String = f"""INSERT INTO {table} 
                        (batch_id, job_status, creation_time, tracking_reference) 
                        VALUES ('{batch_id}', '{status}', '{created_at}', 'TRACK');
                    """
        
        cursor.execute(sql_String)
        cursor.connection.commit()
        return True
    
    except Exception as e:
        cursor.connection.rollback()
        raise "Error in updating Tracking DB, Rolling Back"
    
    finally:
        cursor.connection.close()

def update_batch_information(batch_response, cursor):
    
    if os.getenv("run_config", None) == "server":
        return update_batch_postgres(batch_response, cursor)

    elif os.getenv("run_config", None) == "local":
        return update_batch_local(batch_response, cursor)
    
    else:
        raise "Try Catch Exception: No Valid run_config found while updating"


def export_batch_file(filePath, client):
    
    print("*" * 8, "Submit Batch Request")
    file = client.files.create(
                        file=open(filePath, "rb"), 
                        purpose="batch"
                )

    file_id = file.id

    batch_response = client.batches.create(
                                            input_file_id=file_id,
                                            endpoint="/chat/completions",
                                            completion_window="24h",
                                        )
    
    return batch_response


def create_batchRequest_db(name: str):

    print("*" * 8, "Create Tracking DB")
    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(f'{name}.db')
    cursor = conn.cursor()

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {name} (
            batch_id TEXT,
            job_status TEXT,
            creation_time TEXT,
            tracking_reference TEXT
        )
    ''')

    return cursor

def check_postgres_connection():

    print("*" * 8, "Check Posstgres Connection")
    schema_name = os.getenv("track_db_schema")
    try:
        conn_params_track = {
                'host': os.getenv("hostName_dev", None), 
                'port': '5432', 
                'dbname': os.getenv("track_db_name", None), 
                'user': os.getenv("user_name", None), 
                'password': os.getenv("password", None)
        }
        conn = psycopg2.connect(**conn_params_track)
        cursor = conn.cursor()
        cursor.execute(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{schema_name}'
                """)
        tables = cursor.fetchall()
        table_names = [i[0] for i in tables]
        
        if len(table_names) == 0:
            return None
        
        else:
            return cursor

    except Exception as e:
        return None

def check_sqlite_connection():

    name = "track_status"
    print("*" * 8, "Check DB Connection")
    try:
        conn = sqlite3.connect(f'{name}.db')
        cursor = conn.cursor()
        cursor.execute(f"""
                SELECT count(name) 
                FROM sqlite_master 
                WHERE type='table' AND name='{name}';
            """)
        
        if cursor.fetchone()[0] == 0:
            return None
        
        else:
            return cursor

    except Exception as e:
        return None

def check_db_connection():

    if os.getenv("run_config", None) == "server":
        return check_postgres_connection()
    
    elif os.getenv("run_config", None) == "local":
        return check_sqlite_connection()
    
    else:
        raise "Try Catch Exception: No Valid Tracking DB Connection found"
        

def view_db_connection(name: str, conn):

    df = pd.read_sql_query(f"SELECT * from {name} limit 10", conn)
    print(df)
    return df

