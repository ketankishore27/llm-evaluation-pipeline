import os
import asyncio
from utils.llm_operations_langchain import client
from utils.data_processing_create import get_data, create_batch_file, \
                                  process_all_senders, export_batch_file, \
                                  update_batch_information, check_db_connection, \
                                  create_batchRequest_db

if __name__ == "__main__":

    # ## Get All Data from DB

    data = get_data() ## ToModify
    # # data = get_data(mode="csv", filePath="data/sampleFile.csv") ## ToModify

    # ## Process Text for each conversations
    results = asyncio.run(process_all_senders(data))

    # ## Create JsonL file to make batch request    
    create_batch_file(results)

    ## Check Db Connections and get cursor
    cursor = check_db_connection()
    if not cursor:
        if os.getenv("run_config", None) == "local":
            TRACK_DB = "track_status"
            cursor = create_batchRequest_db(TRACK_DB)
        else:
            raise ("Tracking DB not connected")

    ## Create batch request in OpenAI Space
    batch_response = export_batch_file(filePath = "messages.jsonl", client = client)

    ## Update Batch info
    status = update_batch_information(batch_response, cursor=cursor)

    if status:
        print("Batch Request Created")



