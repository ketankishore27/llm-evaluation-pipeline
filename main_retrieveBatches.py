import sys, os
from utils.llm_operations_langchain import client
from utils.data_processing_retrieve import get_tracking_connections, append_records, \
                                           retrieve_batch_completions, json_result_decode, \
                                           check_scoresDB_connection, update_track_status


if __name__ == "__main__":

    ## Check connections to batch request db
    
    df, engine_tracking = get_tracking_connections()
    if df.shape[0] == 0:
        sys.exit("Nothing to track")
        
    ## Check connections to scores db
    engine_scores = check_scoresDB_connection()

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
                status = append_records(engine_scores, completion_results)
                
                ## Update the status in tracking DB
                if status:
                    update_track_status(engine_tracking, batch_id)
                    print(f"Retrieved Result: BatchId - {batch_id}")

        else:
            print(f"Retrieved Result: BatchId - {batch_id} not ready yet")