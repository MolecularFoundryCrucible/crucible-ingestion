# packages
import logging
import requests
from .constants import sql_import_attr, sql_export_attr
import orjson
from crucible.models import Dataset
from .utils import sanitize_metadata
from .ingestors.registry import find_supported_ingestor
from .packet import IngestionPacket
from .client import get_client
logger = logging.getLogger(__name__)

def find_existing_sample(sample):
    """Return the Crucible record for this sample if it already exists, else None.

    unique_id is authoritative when the ingestor supplied one. Falling back to the
    name is only safe for samples with no id, since names are not unique within a
    project.
    """
    unique_id = sample.get('unique_id')
    if unique_id:
        try:
            return get_client().samples.get(unique_id)
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code == 404:
                return None
            raise

    found = get_client().samples.list(sample_name=sample['sample_name'],
                                project_id=sample.get('project_id'))
    if len(found) > 1:
        raise ValueError(
            f"unique_id not provided and sample name {sample['sample_name']!r} is ambiguous "
            f"in project {sample.get('project_id')!r}: {[s['unique_id'] for s in found]}"
        )
    return found[0] if found else None


def populate_existing_ds_info(ig, populate_fields):
    try:
        found_ds = get_client().datasets.get(ig.unique_id, include_metadata=True)
        assoc_files = get_client().datasets.list_files(ig.unique_id)
        logger.info(f'{found_ds=}')

    except requests.exceptions.HTTPError as err:
        if err.response is None or err.response.status_code != 404:
            raise
        logger.info(f'no existing dataset record for {ig.unique_id}')
        found_ds, assoc_files = None, []

    if found_ds is None:
        return ig, None
    
    # populate required info in IG
    for k in populate_fields:
        if found_ds[k] is None:
            continue
        elif found_ds[k] == "":
            continue
        else:
            setattr(ig, k, found_ds[k])

    # populate associated files
    for af in assoc_files:
        ig.associated_files[af['filename']] = {'size': af['size'], 
                                               'sha256_hash': af['sha256_hash']}
    return ig, found_ds
        

def parse(dataset_to_process, dsid, ingestion_class=None):    
    logger.info("running build packet...")

    ig, ingestion_class = find_supported_ingestor(dataset_to_process, dsid, ingestion_class)
    if ig is None:
        logger.warning("Tried all ingestors with no matches found")
        return None


    # check if the dataset already exists; reinstantiate ig with info
    populate_fields = ['dataset_name', 'public', 'owner_orcid',
                       'project_id', 'measurement', 'session_name',
                       'instrument_name', 'data_type', 'timestamp',
                       'data_format', 'size']
    
    ig, found_ds = populate_existing_ds_info(ig, populate_fields)
        
    # parse the file + add any additional metadata
    try:
        ig.setup_data()
    finally:
        ig.cleanup()

    # if found; overwrite parsed data with what already existed in SQL
    # to overwrite use "update" endpoint; not "ingestion-request"
    if found_ds:
        ig.to_ig_from_sql(found_ds, sql_import_attr)
        logger.info("updated Ingestor object with found data")

        # After merging Crucible scientific_metadata, honour any uploader-specified exclusions.
        # "skipped thin films" is only present for Nirvana "from file" mode with user exclusions.
        skipped = set(ig.scientific_metadata.get("skipped thin films", []))
        for sample in ig.samples:
            if sample.get("unique_id") in skipped:
                sample["link_to_dataset"] = False
        if skipped:
            ig.children = [c for c in ig.children
                           if not set(c.get("sample_links", [])).intersection(skipped)]

    else:
        logger.info("no dataset found to update from")

    ingestion_class = ig.ingestion_class
    md = orjson.loads(orjson.dumps(sanitize_metadata(ig.scientific_metadata),
                                   option=orjson.OPT_SERIALIZE_NUMPY))

    skip_fields = {'keywords', 'ingestion_class', 'thumbnails', 
                   'scientific_metadata', 'acl', 'ingestion_githash'}
    
    D = {k: getattr(ig, k) for k in sql_export_attr if k not in skip_fields}

    return IngestionPacket(
            unique_id=ig.unique_id,
            ingestion_class=ig.ingestion_class,
            dataset_fields=D, 
            scientific_metadata=md,
            keywords=[kw for kw in ig.keywords if isinstance(kw, str) and kw != ""],
            samples=ig.samples,
            children=ig.children,
            thumbnails=ig.thumbnails,
        )


def push_packet(packet):
    # send the data
    ds = get_client().datasets.update(packet.unique_id, **packet.dataset_fields)

    # link to any parsed samples
    for sample in packet.samples:
        logger.info(f'{sample=}')
        # read without mutating: the packet must survive a retry or a second push
        sample_parents = sample.get('parent_ids', [])
        link_to_dataset = sample.get('link_to_dataset', True)
        sample_fields = {k: v for k, v in sample.items()
                         if k not in ('parent_ids', 'link_to_dataset')}

        # get or create sample; re-ingesting the same file must not duplicate samples
        sql_sample = find_existing_sample(sample_fields)
        if sql_sample:
            logger.info(f'found existing sample {sql_sample}')
        else:
            sql_sample = get_client().samples.create(**sample_fields)
            logger.info(f'created new sample {sql_sample}')

        # link to dataset
        if link_to_dataset is True:
            get_client().datasets.add_sample(dataset_id = ds['unique_id'], 
                                             sample_id = sql_sample['unique_id'])

        # link to parents if listed
        for parent in sample_parents:
            get_client().samples.link(parent_id = parent,
                                      child_id = sql_sample['unique_id'])

    # add children
    existing_children = {}
    # a child may name an earlier child as its parent, using the unique_id the ingestor assigned.
    # That id is only real once the child is created, and is discarded entirely when the child
    # turns out to already exist, so every parent_id is read through this map.
    resolved_ids = {}
    for child in packet.children:
        child_ds = child['dataset']
        child_md = child['scientific_metadata']
        parent_id = resolved_ids.get(child['parent_id'], child['parent_id'])
        sample_ids = child['sample_links']
        declared_id = child_ds.get('unique_id')

        # re-pushing the same file must not create a second set of children
        if parent_id not in existing_children:
            existing_children[parent_id] = {c['dataset_name']: c['unique_id']
                                            for c in get_client().datasets.list_children(parent_id)}

        found_dsid = existing_children[parent_id].get(child_ds['dataset_name'])
        if found_dsid:
            logger.info(f"child {child_ds['dataset_name']!r} already exists as {found_dsid}; skipping")
            if declared_id:
                resolved_ids[declared_id] = found_dsid
            continue

        resp = get_client().datasets.create(Dataset(**child_ds), child_md)
        child_dsid = resp['dsid']
        existing_children[parent_id][child_ds['dataset_name']] = child_dsid
        if declared_id:
            resolved_ids[declared_id] = child_dsid

        # link to run dataset
        get_client().datasets.link_parent_child(parent_dataset_id = parent_id,
                                                child_dataset_id = child_dsid)

        # link to thin film
        for sample_id in sample_ids:
            get_client().datasets.add_sample(dataset_id = child_dsid,
                                             sample_id = sample_id)

        for thumbnail in child.get('thumbnails', []):
            try:
                get_client().datasets.add_thumbnail(child_dsid,
                                                    thumbnail['thumbnail'],
                                                    thumbnail['caption'])
            except Exception as err:
                logger.error(f"Failed to add child thumbnail with error {err}")


    # thumbnails
    for thumbnail in packet.thumbnails:
        try:
            logger.info(f"Adding thumbnail image: {thumbnail['caption']=}")
            res = get_client().datasets.add_thumbnail(packet.unique_id, 
                                                      thumbnail['thumbnail'],
                                                      thumbnail['caption'])
        except Exception as err:
            logger.error(f"Failed to add thumbnail with error {err}")
    
    # keywords
    for kw in packet.keywords:
        try:
            get_client().datasets.add_keyword(packet.unique_id, kw)
        except Exception as err:
            logger.error(f"Failed to add keyword {kw} with error {err}")
    
    logger.info(f"Keyword addition complete Added these keywords: {packet.keywords}")

    # scientific metadata
    logger.info(f"Updating scientific metadata with {packet.scientific_metadata=}")
    res = get_client().datasets.update_scientific_metadata(packet.unique_id,
                                                           packet.scientific_metadata,
                                                           overwrite = False)
    
    logger.info(f"Scientific metadata update complete. Response: {res}")
    return ds


def data_ingestion(dataset_to_process, dsid, ingestion_class=None):
    
    packet = parse(dataset_to_process, dsid, ingestion_class)

    if packet is None:
        return (None, None)
    return push_packet(packet), packet.ingestion_class



    