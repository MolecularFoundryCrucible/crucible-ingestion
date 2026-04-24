# packages
import json
import logging

from crucible.utils.io import checkhash
from constants import sql_import_attr

logger = logging.getLogger(__name__)

from ingestors.scope_foundry_ingestors import ( SimpleTiledImageScopeFoundryH5Ingestor,
                                                BioGlowIngestor,
                                                QSpleemSVRampIngestor, 
                                                QSpleemImageIngestor, 
                                                QSpleemARRESEKIngestor,
                                                QSpleemARRESMMIngestor, 
                                                CanonCaptureScopeFoundryH5Ingestor, 
                                                SingleSpecScopeFoundryH5Ingestor,
                                                HyperspecScopeFoundryH5Ingestor,
                                                HyperspecSweepScopeFoundryH5Ingestor,
                                                ToupcamLiveScopeFoundryH5Ingestor,
                                                CLSyncRasterScanIngestor,
                                                CLHyperspecIngestor, 
                                                SpinbotSpecLineIngestor,
                                                SpinbotSpecRunIngestor,
                                                SpinbotCameraCaptureIngestor, 
                                                SpinbotPhotoRunIngestor,
                                                NirvanaMultiPosLineScanIngestor)

from ingestors.image_ingestor import ImageIngestor
from ingestors.insitu_pl_ingestor import InSituPlIngestor
from ingestors.dm_ingestor import DigitalMicrographIngestor
from ingestors.ser_ingestor import SerIngestor
from ingestors.bcf_ingestor import BcfIngestor
from ingestors.emd_ingestor import BerkeleyEmdIngestor
from ingestors.emd_velox_ingestor import VeloxEmdIngestor
from ingestors.jupiter_afm_ingestor import AFMIngestor
from ingestors.czi_ingestor import CziIngestor
from ingestors.ptychography_h5_ingestor import PtychographyH5Ingestor
from ingestors.api_upload_ingestor import ApiUploadIngestor

logger.info("imported all classes")
ingestor_list = [AFMIngestor,
                PtychographyH5Ingestor,
                SimpleTiledImageScopeFoundryH5Ingestor, 
                BioGlowIngestor,
                QSpleemSVRampIngestor, 
                QSpleemImageIngestor, 
                QSpleemARRESEKIngestor,
                QSpleemARRESMMIngestor, 
                CanonCaptureScopeFoundryH5Ingestor, 
                SingleSpecScopeFoundryH5Ingestor,
                HyperspecScopeFoundryH5Ingestor,
                HyperspecSweepScopeFoundryH5Ingestor,
                ToupcamLiveScopeFoundryH5Ingestor,
                CLSyncRasterScanIngestor,
                CLHyperspecIngestor, 
                SpinbotSpecLineIngestor,
                SpinbotCameraCaptureIngestor, 
                SpinbotPhotoRunIngestor, 
                InSituPlIngestor,
                CziIngestor,
                DigitalMicrographIngestor,
                SerIngestor,
                BcfIngestor,
                BerkeleyEmdIngestor,
                VeloxEmdIngestor,
                SpinbotSpecRunIngestor,
                ImageIngestor,
                NirvanaMultiPosLineScanIngestor] 



def find_supported_ingestor(dataset_to_process,
                            dsid,
                            specified_ingestor = None,
                            ingestor_list = ingestor_list):
    
    if specified_ingestor is not None:
        cls = globals()[specified_ingestor]
        logger.info(cls)
        ig = cls(file_to_upload = dataset_to_process, unique_id = dsid)
        if ig.is_file_supported():
            logger.info(f"{dataset_to_process} is supported by {specified_ingestor}")
            return ig
        else:
            logger.warning(f"{dataset_to_process} not supported by {specified_ingestor}")

    # if that ingestor class was not supported, check the others
    for ingestor_class in ingestor_list:
        ig = ingestor_class(file_to_upload = dataset_to_process, unique_id = dsid)

        if ig.is_file_supported():
            logger.info(f"{dataset_to_process} is supported by {ingestor_class.__name__}")
            return ig
        else:
            continue

    return None


def populate_existing_ds_info(ig, dataset_to_process, client, populate_fields):
    found_ds = client.datasets.get(ig.unique_id, include_metadata=True)

    if not found_ds:
        ig.sha256_hash_file_to_upload = checkhash(dataset_to_process)
        found_ds = client.datasets.get(ig.sha256_hash_file_to_upload, include_metadata = True)

    # add required info to IG
    if found_ds:
        for k in populate_fields:
            if found_ds[k] is None:
                continue
            elif found_ds[k] == "":
                continue
            else:
                setattr(ig, k, found_ds[k])

    assoc_files = client.get_associated_files(ig.unique_id)
    logger.info(f'{ig.unique_id}: {assoc_files=}')
    for af in assoc_files:
        ig.associated_files[af['filename']] = {'size': af['size'], 
                                               'sha256_hash': af['sha256_hash']}
    return ig, found_ds
        

def data_ingestion(dataset_to_process: str,
                   dsid: str,
                   reqid: str,
                   timestamp: str,
                   client = None,
                   ingestion_class=None):
    
    logger.info("running the data_ingestion function")
    
    # set up
    storage_bucket = 'mf-storage-prod'
    ingest_json_fname = f"{dsid}_ingest_{timestamp}_{reqid}.json"


    # select ingestion class to use
    # if no supporting ingestion class found; 
    # request will get rerouted to "not-supported queue"
    ig = find_supported_ingestor(dataset_to_process, dsid, ingestion_class, ingestor_list)
    if ig is None:
        logger.warning("Tried all ingestors with no matches found")
        return (None, None)

    # check if the dataset already exists; reinstantiate ig with info
    populate_fields = ['owner_orcid', 'project_id', 'measurement', 'session_name', 'instrument_name'] #'owner_user_id', 'instrument_id',
    ig, found_ds = populate_existing_ds_info(ig, dataset_to_process, client, populate_fields)
        
    # parse the file + add any additional metadata
    ig.setup_data()

    # if found; overwrite parsed data with what already existed in SQL
    # to overwrite use "update" endpoint; not "ingestion-request"
    if found_ds:
        ig.to_ig_from_sql(found_ds, sql_import_attr) 
        logger.info("updated Ingestor object with found data")

    else:
        logger.info("no dataset found to update from")
    
    
    # send to gcs
    num_files = len(ig.associated_files)
    use_n_cores = min(32, int(num_files / 4)+1)
    ig.to_google_cloud_storage(storage_bucket,
                               jsonfile = ingest_json_fname,
                               copy_assoc_files = True,
                               num_cores = use_n_cores)
    logger.info(f"Created json file {ingest_json_fname=} and copied to GCS")
    
    # only do this if ingestor found: update SQL database
    with open(ingest_json_fname) as j:
        D = json.load(j)

    keywords = D.pop('keywords') 
    acl = D.pop('acl')
    associated_files = D.pop('associated_files')
    thumbnails = D.pop('thumbnails')
    md = D.pop("scientific_metadata") 
    #logger.info(f"Data to update: {D}")

    # send the data
    ds = client.datasets.update(ig.unique_id, **D)
    #logger.info(f"UPDATED DS: {ds=}")

    # thumbnails
    for thumbnail in thumbnails:
        try:
            logger.info(f"Adding thumbnail image: {thumbnail['caption']=}")
            res = client.datasets.add_thumbnail(dsid, thumbnail['thumbnail'], thumbnail['caption'])
        except Exception as err:
            logger.error(f"Failed to add thumbnail with error {err}")

    # associated files
    print(f"Adding associated files: {associated_files=}")
    for filepath, file_info in associated_files.items():
        try:
            logger.info({"filename": filepath, "size": file_info['size'], "sha256_hash": file_info['sha256_hash']})
            associated_file_data = {
                        'filename': filepath,
                        'size': file_info['size'],
                        'sha256_hash': file_info['sha256_hash']
                    }
            client._request('post', f'/datasets/{dsid}/associated_files', json=associated_file_data)
        except Exception as err:
            logger.error(f"Failed to add associated file with error {err}")

    # keywords
    filt_keywords = [kw for kw in keywords if isinstance(kw, str) and kw != ""]
    for kw in filt_keywords:
        try:
            client.add_dataset_keyword(dsid, kw)
        except Exception as err:
            logger.error(f"Failed to add keyword {kw} with error {err}")
    
    logger.info(f"Keyword addition complete Added these keywords: {keywords}")

    # scientific metadata
    res = client.datasets.update_scientific_metadata(dsid, md, overwrite = False)
  #  logger.info(f"Scientific metadata update complete. Response: {res}")






    