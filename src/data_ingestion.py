# packages
import logging
from .constants import sql_import_attr, sql_export_attr
import orjson
from .utils import sanitize_metadata

from .ingestors.scope_foundry_ingestors import ( SimpleTiledImageScopeFoundryH5Ingestor,
                                                BioGlowIngestor,
                                                QSpleemSVRampIngestor,
                                                QSpleemSVRampSpinIngestor,
                                                QSpleemImageIngestor,
                                                QSpleemDepositionMonitorIngestor,
                                                QSpleemSPLEEMImageIngestor,
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
                                                NirvanaMultiPosLineScanIngestor,
                                                ScopeFoundryH5Ingestor)

from .ingestors.image_ingestor import ImageIngestor
from .ingestors.insitu_pl_ingestor import InSituPlIngestor
from .ingestors.dm_ingestor import DigitalMicrographIngestor
from .ingestors.emi_ingestor import EmiIngestor
from .ingestors.ser_ingestor import SerIngestor
from .ingestors.bcf_ingestor import BcfIngestor
from .ingestors.emd_ingestor import BerkeleyEmdIngestor
from .ingestors.emd_velox_ingestor import VeloxEmdIngestor
from .ingestors.jupiter_afm_ingestor import AFMIngestor
from .ingestors.czi_ingestor import CziIngestor
from .ingestors.ptychography_h5_ingestor import PtychographyH5Ingestor
from .ingestors.h5_ingestor import H5Ingestor
from .ingestors.api_upload_ingestor import ApiUploadIngestor

logger = logging.getLogger(__name__)
logger.info("imported all classes")
ingestor_list = [AFMIngestor,
                PtychographyH5Ingestor,
                SimpleTiledImageScopeFoundryH5Ingestor, 
                BioGlowIngestor,
                QSpleemSVRampIngestor,
                QSpleemSVRampSpinIngestor,
                QSpleemImageIngestor,
                QSpleemSPLEEMImageIngestor,
                QSpleemDepositionMonitorIngestor,
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
                EmiIngestor,
                SerIngestor,
                BcfIngestor,
                BerkeleyEmdIngestor,
                VeloxEmdIngestor,
                SpinbotSpecRunIngestor,
                ImageIngestor,
                NirvanaMultiPosLineScanIngestor,
                ScopeFoundryH5Ingestor,
                H5Ingestor] 



def find_supported_ingestor(dataset_to_process,
                            dsid,
                            specified_ingestor = None,
                            ingestor_list = ingestor_list):
    
    if specified_ingestor is not None:
        cls = globals().get(specified_ingestor)
        if cls is None:
            logger.warning(f"Specified ingestor '{specified_ingestor}' not found, falling back to list scan")
        else:
            logger.info(cls)
            ig = cls(file_to_upload = dataset_to_process, unique_id = dsid)
            if ig.is_file_supported():
                logger.info(f"{dataset_to_process} is supported by {specified_ingestor}")
                return ig, specified_ingestor
            else:
                logger.warning(f"{dataset_to_process} not supported by {specified_ingestor}")

    # if that ingestor class was not supported, check the others
    for ingestor_class in ingestor_list:
        ig = ingestor_class(file_to_upload = dataset_to_process, unique_id = dsid)

        if ig.is_file_supported():
            logger.info(f"{dataset_to_process} is supported by {ingestor_class.__name__}")
            return ig, ingestor_class.__name__
        else:
            continue

    return None, None


def populate_existing_ds_info(ig, client, populate_fields):
    found_ds = client.datasets.get(ig.unique_id, include_metadata=True)

    # add required info to IG
    if found_ds:
        for k in populate_fields:
            if found_ds[k] is None:
                continue
            elif found_ds[k] == "":
                continue
            else:
                setattr(ig, k, found_ds[k])

    assoc_files = client.datasets.get_associated_files(ig.unique_id)
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

    ig, ingestion_class = find_supported_ingestor(dataset_to_process, dsid, ingestion_class, ingestor_list)
    if ig is None:
        logger.warning("Tried all ingestors with no matches found")
        return (None, None)


    # check if the dataset already exists; reinstantiate ig with info
    populate_fields = ['dataset_name', 'public', 'owner_orcid',
                       'project_id', 'measurement', 'session_name',
                       'instrument_name', 'data_type', 'timestamp',
                       'data_format', 'size', 'source_folder']
    
    ig, found_ds = populate_existing_ds_info(ig, client, populate_fields)
        
    # parse the file + add any additional metadata
    ig.setup_data()

    # if found; overwrite parsed data with what already existed in SQL
    # to overwrite use "update" endpoint; not "ingestion-request"
    if found_ds:
        ig.to_ig_from_sql(found_ds, sql_import_attr) 
        logger.info("updated Ingestor object with found data")

    else:
        logger.info("no dataset found to update from")
    
    
    keywords = ig.keywords
    ingestion_class = ig.ingestion_class
    thumbnails = ig.thumbnails
    md = orjson.loads(orjson.dumps(sanitize_metadata(ig.scientific_metadata), option=orjson.OPT_SERIALIZE_NUMPY))

    skip_fields = {'keywords', 'ingestion_class', 'thumbnails', 'scientific_metadata', 'acl', 'ingestion_githash'}
    D = {k: getattr(ig, k) for k in sql_export_attr if k not in skip_fields}

    # send the data
    ds = client.datasets.update(ig.unique_id, **D)

    # link to any parsed samples
    for sample in ig.samples:
        logger.info(f'{sample=}')
        sql_sample = client.samples.create(**sample)
        client.datasets.add_sample(dataset_id = ds['unique_id'], sample_id = sql_sample['unique_id'])

    # thumbnails
    for thumbnail in thumbnails:
        try:
            logger.info(f"Adding thumbnail image: {thumbnail['caption']=}")
            res = client.datasets.add_thumbnail(dsid, thumbnail['thumbnail'], thumbnail['caption'])
        except Exception as err:
            logger.error(f"Failed to add thumbnail with error {err}")
    
    # keywords
    filt_keywords = [kw for kw in keywords if isinstance(kw, str) and kw != ""]
    for kw in filt_keywords:
        try:
            client.datasets.add_keyword(dsid, kw)
        except Exception as err:
            logger.error(f"Failed to add keyword {kw} with error {err}")
    
    logger.info(f"Keyword addition complete Added these keywords: {keywords}")

    # scientific metadata
    res = client.datasets.update_scientific_metadata(dsid, md, overwrite = False)
    logger.info(f"Scientific metadata update complete. Response: {res}")
    return (ds, ingestion_class)





    