# packages
# Allow running this module directly (python dry_run_data_ingestion.py) while
# keeping the relative imports below working by establishing the package context.
if __package__ in (None, ""):
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "src"

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

from .ingestors.rga_tey_batch_ingestor import RgaTeyBatchIngestor
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
from .ingestors.autobot_spinrun_ingestor import SpinRunIngestor

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
                RgaTeyBatchIngestor,
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
                SpinRunIngestor,
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

    assoc_files = client.datasets.list_files(ig.unique_id)
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
    logger.info(f'call client.datasets.update({ig.unique_id=}, {D=})')

    # link to any parsed samples
    for sample in ig.samples:
        logger.info(f'{sample=}')
        sample_parents = []
        if 'parent_ids' in sample:
            sample_parents = sample.pop('parent_ids')
        
        # create sample
        try:
            logger.info(f'call client.samples.create({sample=})')
        except:
            existing_samples = client.samples.list(sample_name = sample['sample_name'],
                                                   project_id = sample['project_id'])
            if len(existing_samples) > 0:
                sql_sample = existing_samples[-1]
            else:
                sql_sample = None

        # link to dataset
        logger.info(f'linking {ig.unique_id=} to {sample["unique_id"]=}')

        # link to parents if listed
        for parent in sample_parents:
            logger.info(f'linking {parent=} to {sample["unique_id"]=}')

    # add children
    for child in ig.children:
        child_ds = child['dataset']
        md = child['scientific_metadata']
        parent_id = child['parent_id']
        sample_ids = child['sample_links']
        logger.info(f'Creating {child_ds=} with {md=}')
        
        # link to run dataset
        logger.info(f'linking to {parent_id=}')

        # link to thin film
        for sample_id in sample_ids:
            logger.info(f'linking to {sample_id=}')

    # thumbnails
    for thumbnail in thumbnails:
        try:
            logger.info(f"Adding thumbnail image: {thumbnail['caption']=}")
        except Exception as err:
            logger.error(f"Failed to add thumbnail with error {err}")
    
    # keywords
    filt_keywords = [kw for kw in keywords if isinstance(kw, str) and kw != ""]
    for kw in filt_keywords:
        try:
            logger.info(f'adding keyword {kw}')
        except Exception as err:
            logger.error(f"Failed to add keyword {kw} with error {err}")
    
    logger.info(f"Keyword addition complete Added these keywords: {keywords}")

    # scientific metadata
    logger.info(f"Updating scientific metadata with {md=}")


if __name__ == '__main__':
    import os
    import sys
    from crucible import CrucibleClient

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 4:
        raise SystemExit(
            "usage: python -m dry_run_data_ingestion <dataset_path> <dsid> <ingestor>"
        )

    dataset_to_process = sys.argv[1]
    dsid = sys.argv[2]
    ingestion_class = sys.argv[3]
    logger.info(f"{dataset_to_process=}")
    logger.info(f"{dsid=}")
    logger.info(f"{ingestion_class=}")

    client = CrucibleClient()

    data_ingestion(
        dataset_to_process=dataset_to_process,
        dsid=dsid,
        reqid=None,
        timestamp=None,
        client=client,
        ingestion_class=ingestion_class,
    )



    