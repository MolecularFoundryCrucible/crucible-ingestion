import os
import json
import time
import logging
from crucible import CrucibleClient

from .utils import get_secret, setup_pika_client
from crucible.utils.io import get_tz_isoformat
from .data_ingestion import data_ingestion

crucible_api_url = os.environ.get('CRUCIBLE_API_URL')
ingestion_githash = os.environ.get('GITHASH')
rmq_host = os.environ.get('RMQ_HOST')
rmq_port = os.environ.get('RMQ_PORT')
RMQ_ROUTING_SUFFIX = os.environ.get('RMQ_ROUTING_SUFFIX')

logger = logging.getLogger(__name__)

# Vars ===========================
rmq_pw = get_secret("RABBITMQ_DEFAULT_PW", "rabbitmq_default_pw/versions/1")
crucible_apikey = get_secret("ADMIN_APIKEY", "crucible_admin_apikey/versions/4")

num_cores = os.cpu_count()

# RMQ Setup ===========================
connection, channel = setup_pika_client(rmq_host, rmq_port, rmq_pw)

queues_needed = [f'ingestion-{RMQ_ROUTING_SUFFIX}', 'not-supported', f'ingestion-{RMQ_ROUTING_SUFFIX}-failed']

for q in queues_needed:
    channel.queue_declare(queue=q)

client = CrucibleClient(api_url=crucible_api_url, api_key=crucible_apikey)


# Functions ===========================
def is_file_lost(message, dataset_to_process, ch, update_status=True):

    reqid = message['reqid']
    dsid = message['dsid']
    file_exists = os.path.exists(dataset_to_process)
    if not file_exists:
        if update_status:
            client.files.update_ingestion_status(reqid, status = "file not found")
        file_lost = True

    else:
        file_lost = False

    return file_lost

def callback(ch, method, props, body):
    '''
    Expects a RMQ message with: 
    
    filename: The path in GCS to get the file that you want to ingest from
    reqid:    The ingestion request ID
    dsid:     The dataset ID that the ingestion request was made for
              and that the new data will be uploaded to

    Will skip requests for files that are: 
        - Not supported by a currently deployed ingestion class

    '''
    # get info
    message = json.loads(body.decode("utf-8").strip())
    filename = message['filename']
    filename = filename.replace('\\', '/')
    if filename.startswith('/mnt/gcs'):
        dataset_to_process = filename
    elif filename.startswith('crucible-uploads'):
        dataset_to_process = filename.replace('crucible-uploads', '/mnt/gcs', 1)
    elif filename.startswith('mf-storage-prod'):
        dataset_to_process = filename.replace('mf-storage-prod', '/mnt/gcs-prod', 1)
    else:
        logger.error(f"Unexpected filename format, cannot resolve path: {filename}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    specified_ingestor = message['ingestion_class']
    reqid = message['reqid']
    dsid = message['dsid']
    start_time = get_tz_isoformat().replace(":", "")
    logger.info(f"received message {message} .. starting processing")
    
    # update the SQL database that the ingestion has begun
    client.files.update_ingestion_status(reqid, status = "started", ingestion_githash = ingestion_githash)

    # check file found (retry up to 5 times)
    max_file_retries = 5
    for attempt in range(1, max_file_retries + 1):
        if not is_file_lost(message, dataset_to_process, ch, update_status=(attempt == max_file_retries)):
            break
        if attempt < max_file_retries:
            logger.warning(f"[x] File not found, retry {attempt}/{max_file_retries} for {body}")
            time.sleep(2 ** attempt)
        else:
            logger.error(f"[x] Received {body} but file not found after {max_file_retries} attempts")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return


    ds, ingestion_class = (None,None)
    try:
        ds, ingestion_class = data_ingestion(dataset_to_process = dataset_to_process, 
                                    dsid = dsid,
                                    reqid = reqid,
                                    timestamp = start_time,
                                    client = client, 
                                    ingestion_class = specified_ingestor)
        
        logger.info(f"{ds=}")
        if ds is None:
            client.files.update_ingestion_status(reqid, status = "not supported", ingestion_githash = ingestion_githash)    
            ch.basic_publish(exchange = '',
                            routing_key= 'not-supported',
                            body=json.dumps(message))
            logger.warning(f"[x] Received {body} and was not a supported a file type - skipping")

        else:
            client.files.update_ingestion_status(reqid, 
                                                 status = "complete",
                                                 ingestion_githash = ingestion_githash,
                                                 ingestion_class = ingestion_class)
            
            logger.info(f"[x] Received {body} and ingested with id: {ds['unique_id']}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)      
        
    except Exception as err:
        logger.error(f"[x] Received {body} but failed with error {err}")
        client.files.update_ingestion_status(reqid,
                                             "failed",
                                             ingestion_githash = ingestion_githash,
                                             ingestion_class = ingestion_class)
        ch.basic_publish(exchange = '', routing_key= f'ingestion-{RMQ_ROUTING_SUFFIX}-failed', body=json.dumps(message))
        ch.basic_ack(delivery_tag=method.delivery_tag)    
        return
        #ch.basic_nack(delivery_tag=method.delivery_tag)      


# subscribe to the queue
channel.basic_qos(prefetch_count=10)  # tune this up
channel.basic_consume(queue=f'ingestion-{RMQ_ROUTING_SUFFIX}',
                      auto_ack=False,
                      on_message_callback=callback)

# always be listening
logger.info('[*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


























