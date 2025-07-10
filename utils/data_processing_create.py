import pandas as pd
import asyncio
from utils.llm_operations_langchain import prompt_exm, parser, create_batch_yaml_sample
import json
import sqlite3
from typing import Literal

def get_data(mode: Literal["db", "csv"], filePath=None):
    print("*" * 8, "Get-Data")

    if mode == "db":
        conn = sqlite3.connect('conversations.db')
        data = pd.read_sql_query("SELECT * FROM conversations", conn)
        conn.close()

    else:
        try:
            data = pd.read_csv(filePath)
        except Exception as e:
            raise "Provide FilePath"
        
    data_sample = data[data['sender_id'].isin(['Ny7i23GjoezOA_h6NjwIK', 'btLyma2P7Yq2Owe9R5O17', 'yCCKo0asCCrhjWVvgCGQw'])]
    return data_sample

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

def update_batch_information(batch_response, cursor, table):

    print("*" * 8, "Update BatchInfo in csv")

    batch_id = batch_response.id
    status = batch_response.status
    created_at = batch_response.created_at

    sql_String = f"""INSERT INTO {table} 
                    (batch_id, job_status, creation_time, tracking_reference) 
                    VALUES ('{batch_id}', '{status}', '{created_at}', 'TRACK');
                """
    
    cursor.execute(sql_String)
    cursor.connection.commit()
        
    return True


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

def check_db_connection(name: str):

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

def view_db_connection(name: str, conn):

    df = pd.read_sql_query(f"SELECT * from {name} limit 10", conn)
    print(df)
    return df

## Sample Batch ID
# batch_response = {
    #                     "id": "batch_3e62a3ce-d7c4-417b-b078-e2044502734c",
    #                     "completion_window": "24h",
    #                     "created_at": 1751743797,
    #                     "endpoint": "/chat/completions",
    #                     "input_file_id": "file-ee1aa3ac317a44258c136e6be0b00040",
    #                     "object": "batch",
    #                     "status": "validating",
    #                     "cancelled_at": None,
    #                     "cancelling_at": None,
    #                     "completed_at": None,
    #                     "error_file_id": "",
    #                     "errors": None,
    #                     "expired_at": None,
    #                     "expires_at": 1751830197,
    #                     "failed_at": None,
    #                     "finalizing_at": None,
    #                     "in_progress_at": None,
    #                     "metadata": None,
    #                     "output_file_id": "",
    #                     "request_counts": {
    #                         "completed": 0,
    #                         "failed": 0,
    #                         "total": 0
    #                     }
    #         }


    ## Sample File ID
    # file = {
    #             "id": "file-ee1aa3ac317a44258c136e6be0b00040",
    #             "bytes": 15264,
    #             "created_at": 1751743680,
    #             "filename": "messages.jsonl",
    #             "object": "file",
    #             "purpose": "batch",
    #             "status": "processed",
    #             "expires_at": None,
    #             "status_details": None
    #             }  




## LookUps

    # sample_result = chain.invoke({"chat_transcript": conversation})
    
    # return [sender_id, conversation_start_time, conversation_end_time, sample_result['score'], 
    #         sample_result["reasoning"], sample_result["satisfaction_label"]]

#   'hwSa9Mae-O_6c0xJc37zF', 'RPMjuJdSxBXIm9vqszJUm',
#   'yCCKo0asCCrhjWVvgCGQw', '-88c_dXBTRYD5ynmnJDAK',
#   'bDcNLASmqhPn0oGJQds8u', 'ouccmypmvI0YVPyQH8WKy',
#   '73-4nZXRdn4baVUxiK8yu', 'tjVMzGJ8n9zgK4Kevrdxo'