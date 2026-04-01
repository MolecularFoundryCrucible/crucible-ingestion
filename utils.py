
import base64
from io import BytesIO
import os
import json
import logging
from PIL import Image
import pika
from dotenv import load_dotenv
import numpy as np
from datetime import datetime
from google.cloud import secretmanager
from google.oauth2 import service_account

from constants import secret_store
from crucible.utils.io import run_shell

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_cloud_secret_selfauth(secret_name):
    '''
    Get a secret from GCS secret manager using the default credentials of the environment. 
    This will only work if you are running in an environment with a service account that has access to the secret. 
    '''
    try:
        # Assume you are running from a GCP service with SA that can auth itself 
        client = secretmanager.SecretManagerServiceClient() 
        # get your secret
        response = client.access_secret_version(name=secret_name)
        secret_value = response.payload.data.decode("UTF-8")
        return(secret_value)

    except Exception as e:
        logger.error(f"Failed to access secret {secret_name} with self-authentication: {e}")
        return None
    

def get_credentials_from_env():
    load_dotenv('../.env')
    env_gcs_sa = os.environ.get("GCS_SA")
    if env_gcs_sa is None:
        return None
    
    J = json.loads(env_gcs_sa)
    with open("temp_creds.json", "w") as f:
        json.dump(J, f)

    credentials = service_account.Credentials.from_service_account_file("temp_creds.json")
    return credentials


def get_secret(secret_env_var, gcs_secret_name = None, sa_creds: str = None, secret_store = secret_store): 
    '''
    Running locally: Edit .env file in ingestion-consumer with the ADMIN_APIKEY for the crucible api
    Running in the cloud: Environment variables will be available through google secret manager

    General: 
    sa_creds defaults to None in which case it is assumed that you are running the function 
    from a GCP instance with a service account that can authenticate itself. 
    Otherwise, sa_creds should be a filepath to the service account credentials.  
    '''

    load_dotenv('../.env')
    secret = os.environ.get(secret_env_var)
    if secret:
        return secret
    
    elif gcs_secret_name is not None:
        gcs_secret_path = f"{secret_store}/{gcs_secret_name}"

        # Try to get it with service account authenticating itself
        secret = get_cloud_secret_selfauth(gcs_secret_path)
        if secret is not None:
            return secret

        # Try to get secret by providing service account credentials
        if sa_creds is not None:
            credentials = service_account.Credentials.from_service_account_file(sa_creds)
        else:
            credentials = get_credentials_from_env()
            
        if credentials is None:
            logger.error("No credentials available to access GCS secret")
            raise Exception("No credentials available to access GCS secret")
            
        client = secretmanager.SecretManagerServiceClient(credentials=credentials) 
        response = client.access_secret_version(name=gcs_secret_path)
        secret = response.payload.data.decode("UTF-8")
        return(secret)

    else:
        logger.error(f"Secret {secret_env_var} not found in environment variables and no GCS secret name provided")
        raise Exception(f"Secret {secret_env_var} not found in environment variables and no GCS secret name provided")


def setup_pika_client(host, port, pw, heartbeat = 60, blocked_connection_timeout = None):
    print("getting credentials")
    credentials = pika.PlainCredentials('admin', pw)
    parameters = pika.ConnectionParameters(host, port, '/', credentials, heartbeat=heartbeat,
                                           blocked_connection_timeout = blocked_connection_timeout)

    print("connecting")
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    return(connection, channel)


def run_rclone_command(source_path= "",
                       destination_path= "",
                       cmd="copy",
                       client_secret = None,
                       sa_creds = None,
                       gcs_config_name = 'mf-cloud-storage',
                       background = False,
                       checkflag = True,
                       cred_file = None):
    
    # GET THE CLIENT SECRET
    if client_secret is None:
        client_secret = get_secret("GCS_CLIENT_SECRET", "gcs_client_secret/versions/1")
        
    if client_secret is None:
        client_secret = os.getenv("GCS_CLIENT_SECRET")

    # GET THE SERVICE ACCOUNT CREDENTIALS
    if cred_file is not None:
        with open(cred_file, "r") as f:
            sa_creds = f.read()
        if len(sa_creds.strip()) == 0:
            sa_creds = None
            
    if sa_creds is None:
        sa_creds = get_secret("GCS_SA", "service-account-cred/versions/6")

    if sa_creds is None:
        sa_creds = os.getenv("GCS_SA")

    # PASS THEM INTO THE RCLONE COMMAND LINE ARGUMENTS
    cmd_args = [f"--gcs-client-id=776258882599-v17f82atu67g16na3oiq6ga3bnudoqrh.apps.googleusercontent.com" ,
                f"--gcs-client-secret={client_secret}",
                f"--gcs-project-number=mf-crucible",
                f"--gcs-service-account-credentials='{sa_creds}'",
                f"--gcs-bucket-policy-only=true",
                f"--gcs-env-auth=true"]

    # CLEAN UP THE COMMAND
    if len(destination_path.strip()) > 0:
            destination_path = f'"{destination_path}"'

    rclone_drives = [x.split(":/")[0] for x in [source_path, destination_path]]

    try:
        rclone_cmd = "   ".join([f'rclone {cmd}'] + cmd_args + [f'"{source_path}" {destination_path}'])
        run_shell_out = run_shell(rclone_cmd, background = background, checkflag=True)
        
    except:
        home = os.getenv("HOME")
        sa_cred_file = f"{home}/.config/mf-crucible-9009d3780383.json"
        J = json.loads(sa_creds)
        with open(sa_cred_file, "w") as f:
            json.dump(J, f)
        source_path, destination_path = (x.replace(":gcs", gcs_config_name) for x in (source_path, destination_path))
        rclone_cmd = f'rclone {cmd} "{source_path}" {destination_path}'
        run_shell_out = run_shell(rclone_cmd, background = background, checkflag=checkflag)
    return(run_shell_out)


def build_b64_thumbnail(image: Image, max_size = (200,200)): 
    image.thumbnail(max_size)
    image.convert("RGB")
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    thumbnail = base64.b64encode(buffered.getvalue()).decode("UTF-8")
    return(thumbnail)


def reduce_filename_and_copy(f, common_file_paths, destination_prefix):
    replace_path = [x for x in common_file_paths if x in f]
    if len(replace_path) > 0:
        rel_file_path = f.replace(f"{replace_path[-1]}/", "")
    else:
        rel_file_path = f

    if not f.startswith('/mnt/gcs'):
        f = f'/mnt/gcs/{f}'

    rclone_out = run_rclone_command(source_path= f, 
                                    destination_path= f"{destination_prefix}/{rel_file_path}",
                                    cmd="copyto", 
                                    checkflag = False)
    
    logger.info(f"{rclone_out.stderr=} for rclone copyto {f} {destination_prefix}/{rel_file_path}")
    return(rclone_out)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.int16):
            return int(obj)
        if isinstance(obj, np.int32):
            return int(obj)
        if isinstance(obj, np.float32):
            return float(obj)
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, np.float64):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint8):
            return int(obj)
        if isinstance(obj, np.uint16):
            return int(obj)
        if isinstance(obj, np.uint32):
            return int(obj)
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, datetime):
            return(str(obj.isoformat()))
        return json.JSONEncoder.default(self, obj)



