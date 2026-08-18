import argparse
import base64
import logging
import os
from io import BytesIO

import mfid
from PIL import Image
from crucible.models import Dataset

from .client import get_client
from .data_ingestion import parse, push_packet

logger = logging.getLogger(__name__)

TEST_PROJECT_ID = "crucible-test"

def main():
    parser = argparse.ArgumentParser(prog="crucible-ingest")
    parser.add_argument("--file", required=True, help="path to the dataset to process")
    parser.add_argument("--dsid", default=None,
                        help="dataset unique id (default: a generated id, dry run only)")
    parser.add_argument("--ingestor", default=None,
                        help="ingestor class name (default: auto-detect)")
    parser.add_argument("--output-dir", dest="output_dir", default=None,
                        help="directory for the parsed packet and thumbnails")
    parser.add_argument("--push", action="store_true",
                        help="push the parsed packet to Crucible instead of only writing it locally")
    parser.add_argument("--test", action="store_true",
                        help="with --push and no --dsid, create a throwaway dataset in "
                             f"{TEST_PROJECT_ID} and push to that")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.push and args.dsid is None and not args.test:
        logger.error("--push needs a dataset to push to. Run `crucible dataset create` and pass "
                     "the new id as --dsid, or re-run with --test to push to a throwaway dataset "
                     f"in {TEST_PROJECT_ID}.")
        return 1

    create_test_dataset = args.push and args.dsid is None
    dsid = args.dsid or mfid.mfid()[0]

    output_dir = args.output_dir or os.path.join(os.getcwd(), "dry_run_output", dsid)
    os.makedirs(output_dir, exist_ok=True)

    # the record has to exist before parsing so that parse() reads project_id back off it;
    # otherwise the push updates the dataset with a null project and untags it
    if create_test_dataset:
        get_client().datasets.create(Dataset(unique_id=dsid, project_id=TEST_PROJECT_ID))
        logger.info(f"no dsid provided, test data will be uploaded to {dsid} "
                    f"in project {TEST_PROJECT_ID}")

    # parse
    packet = parse(args.file, dsid, args.ingestor)
    if packet is None: 
        logger.error(f"no ingestor supports {args.file}")
        return 1

    packet_path = os.path.join(output_dir, "packet.json")
    packet.to_json(packet_path)
    logger.info(f"wrote parsed packet to {packet_path}")

    thumbnails_dir = os.path.join(output_dir, "thumbnails")
    os.makedirs(thumbnails_dir, exist_ok=True)
    for i, thumbnail in enumerate(packet.thumbnails):
        jpg_path = os.path.join(thumbnails_dir, f"thumbnail_{i:03d}.jpg")
        image = Image.open(BytesIO(base64.b64decode(thumbnail["thumbnail"]))).convert("RGB")
        image.save(jpg_path, format="JPEG")
        logger.info(f"saved {thumbnail.get('caption', '')} to {jpg_path}")

    # push
    if args.push:
        ds = push_packet(packet)
        logger.info(f"pushed {ds['unique_id']}")

    return 0
  
  
if __name__ == "__main__":
    raise SystemExit(main())
