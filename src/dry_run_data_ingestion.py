# packages
# Allow running this module directly (python dry_run_data_ingestion.py) while
# keeping the relative imports below working by establishing the package context.
if __package__ in (None, ""):
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "src"

import base64
import json
import logging
import os
from io import BytesIO

from PIL import Image

from .constants import sql_import_attr, sql_export_attr
import orjson
from .utils import sanitize_metadata, EnhancedJSONEncoder
from .data_ingestion import find_existing_sample

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
from .ingestors.inorganic_xrd_ingestor import InorganicXRDIngestor

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
                InorganicXRDIngestor,
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
    try:
        found_ds = client.datasets.get(ig.unique_id, include_metadata=True)
        assoc_files = client.datasets.list_files(ig.unique_id)
    except:
        found_ds = None
        assoc_files = []

    # add required info to IG
    if found_ds:
        for k in populate_fields:
            if found_ds[k] is None:
                continue
            elif found_ds[k] == "":
                continue
            else:
                setattr(ig, k, found_ds[k])

    
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
                   ingestion_class=None,
                   output_dir=None):

    logger.info("running the data_ingestion function")

    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "dry_run_output", str(dsid))
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"dry run outputs will be written to {output_dir}")

    ig, ingestion_class = find_supported_ingestor(dataset_to_process, dsid, ingestion_class, ingestor_list)
    if ig is None:
        logger.warning("Tried all ingestors with no matches found")
        return (None, None)


    # check if the dataset already exists; reinstantiate ig with info
    populate_fields = ['dataset_name', 'public', 'owner_orcid',
                       'project_id', 'measurement', 'session_name',
                       'instrument_name', 'data_type', 'timestamp',
                       'data_format', 'size']
    
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

    # collect everything parsed into a single report for inspection
    report = {
        "dataset_path": dataset_to_process,
        "unique_id": ig.unique_id,
        "ingestion_class": ingestion_class,
        "dataset_fields": D,
        "scientific_metadata": md,
        "keywords": None,
        "samples": [],
        "children": [],
        "thumbnails": [],
    }

    # send the data
    logger.info(f'call client.datasets.update({ig.unique_id=}, {D=})')

    # link to any parsed samples
    for sample in ig.samples:
        report["samples"].append(dict(sample))
        logger.info(f'{sample=}')
        sample_parents = []
        if 'parent_ids' in sample:
            sample_parents = sample.pop('parent_ids')
        
        # get or create sample
        existing = find_existing_sample(client, sample)
        if existing:
            logger.info(f'would reuse existing sample {existing["unique_id"]}')
        else:
            logger.info(f'call client.samples.create({sample=})')

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
        report["children"].append({
            "dataset": child_ds,
            "scientific_metadata": md,
            "parent_id": parent_id,
            "sample_links": sample_ids,
        })
        logger.info(f'Creating {child_ds=} with {md=}')

        # link to run dataset
        logger.info(f'linking to {parent_id=}')

        # link to thin film
        for sample_id in sample_ids:
            logger.info(f'linking to {sample_id=}')

    # thumbnails - decode the base64 payloads and save as jpg files to inspect
    thumbnails_dir = os.path.join(output_dir, "thumbnails")
    os.makedirs(thumbnails_dir, exist_ok=True)
    for i, thumbnail in enumerate(thumbnails):
        try:
            caption = thumbnail.get('caption', '')
            logger.info(f"Adding thumbnail image: {caption=}")
            image = Image.open(BytesIO(base64.b64decode(thumbnail['thumbnail']))).convert("RGB")
            jpg_path = os.path.join(thumbnails_dir, f"thumbnail_{i:03d}.jpg")
            image.save(jpg_path, format="JPEG")
            logger.info(f"Saved thumbnail to {jpg_path}")
            report["thumbnails"].append({"caption": caption, "file": jpg_path})
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
    report["keywords"] = filt_keywords

    # scientific metadata
    logger.info(f"Updating scientific metadata with {md=}")

    # write the collected report to a json file for inspection
    report_path = os.path.join(output_dir, "ingestion_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, cls=EnhancedJSONEncoder, indent=4)
    logger.info(f"Wrote dry run report to {report_path}")


if __name__ == '__main__':
    import argparse
    import os
    from crucible import CrucibleClient

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(prog="dry_run_data_ingestion")
    parser.add_argument("--file", required=True,
                        help="path to the dataset to process")
    parser.add_argument("--dsid", default="xxx",
                        help="dataset unique id (default: xxx)")
    parser.add_argument("--ingestor", default=None,
                        help="ingestor class name (default: auto-detect via find_supported_ingestor)")
    parser.add_argument("--output-dir", dest="output_dir", default=None,
                        help="directory for dry run outputs")
    args = parser.parse_args()

    dataset_to_process = args.file
    dsid = args.dsid
    ingestion_class = args.ingestor
    output_dir = args.output_dir
    logger.info(f"{dataset_to_process=}")
    logger.info(f"{dsid=}")
    logger.info(f"{ingestion_class=}")

    # authenticate with CRUCIBLE_APIKEY; fall back to SDK config if unset
    client = CrucibleClient(
        api_url=os.environ.get('CRUCIBLE_API_URL'),
        api_key=os.environ.get('CRUCIBLE_APIKEY'),
    )

    data_ingestion(
        dataset_to_process=dataset_to_process,
        dsid=dsid,
        reqid=None,
        timestamp=None,
        client=client,
        ingestion_class=ingestion_class,
        output_dir=output_dir,
    )



    