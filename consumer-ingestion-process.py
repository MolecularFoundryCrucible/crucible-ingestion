import os
import json
import time
import logging
from crucible import CrucibleClient

from utils import get_secret, setup_pika_client  
from constants import crucible_api_url, rmq_host, rmq_port                                      
from crucible.utils.io import get_tz_isoformat
from data_ingestion import data_ingestion

ingestion_githash = os.environ.get('GITHASH')
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
            client.datasets.update_ingestion_status(dsid, reqid, "file not found")
        file_lost = True

    else:
        file_lost = False

    return file_lost


def is_file_too_big(message, dataset_to_process, ch):
    reqid = message['reqid']
    dsid = message['dsid']
    fsize = os.path.getsize(dataset_to_process)

    if fsize > 1e10:
        logger.warning(f"[x] Received {message} but sending file to large file queue")
        client.datasets.update_ingestion_status(dsid, reqid, "file too large")

        too_big = True
    else:
        too_big = False
    
    return too_big


def callback(ch, method, props, body):
    '''
    Expects a RMQ message with: 
    
    filename: The path in GCS to get the file that you want to ingest from
    reqid:    The ingestion request ID
    dsid:     The dataset ID that the ingestion request was made for
              and that the new data will be uploaded to

    Will skip requests for files that are: 
        - Not found at the specified path
        - Larger than 2GB
        - Not supported by a currently deployed ingestion class

    Will reroute those files to the queues below: 
        - lost-files
        - large-files
        - not-supported

    '''
    # get info
    message = json.loads(body.decode("utf-8").strip())
    filename = message['filename']
    filename = filename.replace('\\', '/')
    dataset_to_process = filename.replace('crucible-uploads', '/mnt/gcs')
    specified_ingestor = message['ingestion_class']
    reqid = message['reqid']
    dsid = message['dsid']
    start_time = get_tz_isoformat().replace(":", "")
    logger.info(f"received message {message} .. starting processing")
    
    # update the SQL database that the ingestion has begun
    client.datasets.update_ingestion_status(dsid, reqid, "started", ingestion_githash = ingestion_githash)

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

    # check file size
    if is_file_too_big(message, dataset_to_process, ch):
        logger.info(f"[x] Received {body} but file too large")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return  

    ds, ingestion_class = (None,None)
    try:
        # /datasets/{dsid}/upload uploads file to crucible-uploads and returns path to client as crucible-uploads/{path-to-file}
        # client passes the upload path to /datasets/{dsid}/add_associated_file as the filename
        # /add_associated_file uses the associated_file.filename in the ingestion request message
        ds, ingestion_class = data_ingestion(dataset_to_process = dataset_to_process, 
                                    dsid = dsid,
                                    reqid = reqid,
                                    timestamp = start_time,
                                    client = client, 
                                    ingestion_class = specified_ingestor)
        
        logger.info(f"{ds=}")
        if ds is None:
            client.datasets.update_ingestion_status(dsid, reqid, "not supported", ingestion_githash = ingestion_githash)    
            ch.basic_publish(exchange = '',
                            routing_key= 'not-supported',
                            body=json.dumps(message))
            logger.warning(f"[x] Received {body} and was not a supported a file type - skipping")

        else:
            client.datasets.update_ingestion_status(dsid, reqid, "complete", ingestion_githash = ingestion_githash, ingestion_class = ingestion_class)
            logger.info(f"[x] Received {body} and ingested with id: {ds['unique_id']}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)      
        
    except Exception as err:
        logger.error(f"[x] Received {body} but failed with error {err}")
        client.datasets.update_ingestion_status(dsid, reqid, "failed", ingestion_githash = ingestion_githash, ingestion_class = ingestion_class)
        ch.basic_publish(exchange = '', routing_key= f'ingestion-{RMQ_ROUTING_SUFFIX}-failed', body=json.dumps(message))
        ch.basic_ack(delivery_tag=method.delivery_tag)    
        return
        #ch.basic_nack(delivery_tag=method.delivery_tag)      

# subscribe to the queue
channel.basic_consume(queue=f'ingestion-{RMQ_ROUTING_SUFFIX}',
                      auto_ack=False,
                      on_message_callback=callback)

# always be listening
logger.info('[*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


























