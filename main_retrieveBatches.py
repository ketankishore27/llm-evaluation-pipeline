import sys
from utils.llm_operations_langchain import client
from utils.data_processing_retrieve import get_tracking_db_connection, get_batchRequests, \
                                           retrieve_batch_completions, json_result_decode, \
                                           check_resultant_db_connection, create_resultant_db, \
                                           append_records, update_track_status


if __name__ == "__main__":

    ## Check connections to batch request db
    TRACK_DB = "track_status"
    cursor_tracking = get_tracking_db_connection(TRACK_DB)
    if not cursor_tracking:
        sys.exit("No Connection Found")
        
    ## Check connections to scores db
    RESULT_DB = "scores"
    cursor_scores = check_resultant_db_connection(RESULT_DB)
    if not cursor_scores:
        cursor_scores = create_resultant_db(RESULT_DB)

    ## Get Batches to track
    df = get_batchRequests(TRACK_DB, cursor_tracking)

    ## Loop Through batches and process all individually
    for batch_id, job_status, creation_time, tracking_reference in df.values:

        ## Check the individual batch responses
        batch_response = client.batches.retrieve(batch_id)
        if batch_response.status == "completed":
            output_file_id = batch_response.output_file_id
            
            if output_file_id:
                
                ## Get individual completions
                result_set = retrieve_batch_completions(client, output_file_id)

                ## Extract the required parameters
                completion_results = json_result_decode(result_set)

                ## Append records to the SQLite Table
                status = append_records(cursor_scores, RESULT_DB, completion_results)
                
                ## Update the status in tracking DB
                if status:
                    update_track_status(cursor_tracking, TRACK_DB, batch_id)
                    print(f"Retrieved Result: BatchId - {batch_id}")

        else:
            print(f"Retrieved Result: BatchId - {batch_id} not ready yet")