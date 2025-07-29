import os
import json
import pandas as pd
from typing import List, Any
from urllib.parse import quote_plus
from datetime import datetime
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv(override=True)

## For Server: This gets the records which have 'TRACK' status
def get_trackingData_postgres():

    print("*" * 8, "Get Tracking Server")
    try:
        schema_name = os.getenv("track_db_schema", None)
        table_name = os.getenv("track_table_name", None)
        engine = create_engine(f"postgresql+psycopg2://{os.getenv('user_name', None)}:{quote_plus(os.getenv('password', None))}@{os.getenv('hostName_dev', None)}:5432/{os.getenv('track_db_name', None)}")
        data = pd.read_sql(f"SELECT * FROM {schema_name}.{table_name}", engine)
        return data[data['tracking_reference'] == 'TRACK'], engine

    except Exception as e: 
        raise ("Value Error in DB Section, ", e)

## For BATCH: This gets the records which have 'TRACK' status
def get_batchRequests(TRACK_DB: str, cursor) -> pd.DataFrame:
    print("*" * 8, "Get Requests to Track")
    return pd.read_sql_query(f"Select * from {TRACK_DB} where tracking_reference = 'TRACK'", cursor.connection)

def get_trackingDb_sqlite(name: str) -> Any:
    """
    Connects to a local SQLite database and checks for the existence of a table with the given name.

    Args:
        name (str): The name of the SQLite database (without the .db extension) and the table to check.

    Returns:
        sqlite3.Cursor or None: Returns a cursor object if the table exists, otherwise None.
    """

    print("*" * 8, "Check DB Connection")
    try:
        engine = create_engine(f'sqlite:///{name}.db')
        with engine.connect() as conn:
            print(f"Connection Successful to {name}.db in localenv")
        
        return get_batchRequests(name, engine.raw_connection().cursor()), engine

    except Exception as e:
        raise ValueError(f"Connection UnSuccessful to {name}.db in localenv")
    
## Checks the .env file and calls the required 'TRACK'ing function
def get_tracking_connections():

    if os.getenv("run_config", None) == "server":
        return get_trackingData_postgres()

    elif os.getenv("run_config", None) == "local":
        TRACK_DB = "track_status"
        return get_trackingDb_sqlite(TRACK_DB)
    
    else:
        raise "Try Catch Exception: No Valid run_config found while extracting data"


def retrieve_batch_completions(client, output_file_id: str) -> List[Any]:

    print("*" * 8, "Retrieve Batch Completions")
    result_list = []
    file_response = client.files.content(output_file_id)
    raw_responses = file_response.text.strip().split('\n')  

    for raw_response in raw_responses:  
        json_response = json.loads(raw_response)  
        result_list.append(json_response)

    return result_list

def json_result_decode(json_result_set: List[Any]) -> List[List[Any]]:

    print("*" * 8, "Creating structured Records")    
    completion_list = []
    for completion in json_result_set:
        sender_id = completion["custom_id"]
        content = completion['response']['body']['choices'][0]['message']['content']
        claned_content = re.sub(r',\s*([}\]])', r'\1', content)
        json_completion = json.loads(claned_content)
        score = json_completion["score"]
        created_date = str(datetime.fromtimestamp(completion['response']['body']['created']))
        feedback_text = json_completion['reasoning']
        label = json_completion["satisfaction_label"]
        completion_list.append([sender_id, created_date, score, label, feedback_text])

    return completion_list


def check_scoresDb_sqlite(db_name: str):

    print("*" * 8, "Check DB Connection")
    try:
        engine = create_engine(f'sqlite:///{db_name}.db')
        with engine.connect() as conn:
            print(f"Connection Successful to {db_name} in localenv")
        
        return engine

    except Exception as e:
        raise ValueError(f"Connection Unsucessful to {db_name}")


def check_scoresDb_postgres():
    
    print("*" * 8, "Check Posstgres Connection")
    try:
        engine = create_engine(f"postgresql+psycopg2://{os.getenv('user_name', None)}:{quote_plus(os.getenv('password', None))}@{os.getenv('hostName_dev', None)}:5432/{os.getenv('score_db_name', None)}")
        with engine.connect() as conn:
            print(f"Connection Successful to {os.getenv('score_table_name', None)}")

        return engine

    except Exception as e:
        raise ValueError(f"Connection Unsucessful to {os.getenv('score_table_name', None)}")

def check_scoresDB_connection():

    if os.getenv("run_config", None) == "server":
        return check_scoresDb_postgres()
    
    elif os.getenv("run_config", None) == "local":
        RESULT_DB = "scores"
        return check_scoresDb_sqlite(RESULT_DB)
    
    else:
        raise ValueError("Error Encountered in ScoresDB Connection, ")

def append_records_sql(engine, RESULT_TABLE, completion_results: List[List[Any]]):

    print("*" * 8, "Appending Retreived Results")
    df_temp = pd.DataFrame(completion_results, columns=["senderId", "creation_time", "score", "label", "feedback_text"])
    with engine.begin() as conn:
        df_temp.to_sql(name=RESULT_TABLE, con=conn, if_exists="append", index=False)
    return True

def append_records_postgres(engine, completion_results):

    print("*" * 8, "Appending Retreived Results to postgres")
    df_temp = pd.DataFrame(completion_results, columns=["sender_id", "creation_time", "score", "label", "feedback_text"])
    with engine.begin() as conn:
        df_temp.to_sql(name=os.getenv('score_table_name', None), con=conn, if_exists="append", index=False)
    return True

    
    
def append_records(engine, completion_results):

    if os.getenv("run_config", None) == "server":
        return append_records_postgres(engine, completion_results)
    
    elif os.getenv("run_config", None) == "local":
        RESULT_TABLE = "scores"
        return append_records_sql(engine, RESULT_TABLE, completion_results)
    
    else:
        raise ValueError("Error Encountered while appending in ScoresDB Connection, ")

def update_trackStatus_sqlite(engine, db_name: str, batchId: str):

    print("*" * 8, "Update Track Status")
    query = text(f"""
                UPDATE {db_name}
                SET tracking_reference = :tracking_reference
                WHERE batch_id = :batch_id
            """)
    with engine.begin() as conn:
        conn.execute(query, {"tracking_reference": "Completed", "batch_id": batchId})

    return True

def update_trackStatus_postgres(engine, batchId):
    query = text(f"""
                UPDATE {os.getenv('track_table_name', None)}
                SET tracking_reference = :tracking_reference
                WHERE batch_id = :batch_id
            """)

    with engine.begin() as conn:
        conn.execute(query, {"tracking_reference": "Completed", "batch_id": batchId})

    return True

def update_track_status(engine, batch_id):

    if os.getenv("run_config", None) == "server":
        return update_trackStatus_postgres(engine, batch_id)

    elif os.getenv("run_config", None) == "local":
        db_name = "track_status"
        return update_trackStatus_sqlite(engine, db_name, batch_id)

    else:
        raise ValueError("Error while updating the records in `tracking db`")

