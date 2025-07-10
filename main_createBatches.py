import asyncio
from utils.llm_operations_langchain import client
from utils.data_processing_create import get_data, create_batch_file, \
                                  process_all_senders, export_batch_file, \
                                  update_batch_information, check_db_connection, \
                                  create_batchRequest_db, view_db_connection

if __name__ == "__main__":

    # ## Get All Data from DB
    data = get_data(mode="db") ## ToModify
    # # data = get_data(mode="csv", filePath="data/sampleFile.csv") ## ToModify

    # ## Process Text for each conversations
    results = asyncio.run(process_all_senders(data))

    # ## Create JsonL file to make batch request    
    create_batch_file(results)

    ## Create batch request in OpenAI Space
    batch_response = export_batch_file(filePath = "messages.jsonl", client = client)

    ## Check Db Connections and get cursor
    TRACK_DB = "track_status"
    cursor = check_db_connection(TRACK_DB)
    if not cursor:
        cursor = create_batchRequest_db(TRACK_DB)

    ## Update Batch info
    status = update_batch_information(batch_response, cursor=cursor, table=TRACK_DB)

    ## View batch info
    df = view_db_connection(TRACK_DB, cursor.connection)

    ## Close connections
    cursor.connection.close()

    if status:
        print("Batch Request Created")



