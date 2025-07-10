import os
import json
import pandas as pd
from typing import List, Any
import sys
import sqlite3
from datetime import datetime
import re

def get_tracking_db_connection(name: str) -> Any:

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
        print(e)
        return None
    
def get_batchRequests(TRACK_DB: str, cursor) -> pd.DataFrame:
    print("*" * 8, "Get Requests to Track")
    return pd.read_sql_query(f"Select * from {TRACK_DB} where tracking_reference = 'TRACK'", cursor.connection)

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

def check_resultant_db_connection(db_name: str):

    print("*" * 8, "Check DB Connection")
    try:
        conn = sqlite3.connect(f'{db_name}.db')
        cursor = conn.cursor()
        cursor.execute(f"""
                SELECT count(name) 
                FROM sqlite_master 
                WHERE type='table' AND name='{db_name}';
            """)
        
        if cursor.fetchone()[0] == 0:
            return None
        
        else:
            return cursor

    except Exception as e:
        return None
    
def create_resultant_db(db_name: str):

    print("*" * 8, "Create DB")
    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(f'{db_name}.db')
    cursor = conn.cursor()

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {db_name} (
            senderId TEXT,
            creation_time TEXT,
            score TEXT,
            label TEXT,
            feedback_text TEXT
        )
    ''')

    return cursor

def append_records(cursor, RESULT_DB, completion_results: List[List[Any]]):

    print("*" * 8, "Appending Retreived Results")
    df_temp = pd.DataFrame(completion_results, columns=["senderId", "creation_time", "score", "label", "feedback_text"])
    df_temp.to_sql(name=RESULT_DB, con=cursor.connection, if_exists="append", index=False)

    cursor.connection.commit()
    return True

def update_track_status(cursor, db_name: str, batchId: str):

    print("*" * 8, "Update Track Status")
    cursor.execute(f'''
        UPDATE {db_name} 
        SET tracking_reference = "Completed",
        job_status = "Completed"
        where batch_id = '{batchId}'
    ''')

    cursor.connection.commit()
    return True


