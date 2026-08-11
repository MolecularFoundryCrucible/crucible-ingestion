import argparse
import base64
import logging
import os
from io import BytesIO
from PIL import Image

from .data_ingestion import parse, push_packet

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(prog="crucible-ingest")
    parser.add_argument("--file", required=True, help="path to the dataset to process")
    parser.add_argument("--dsid", default="xxx", help="dataset unique id")
    parser.add_argument("--ingestor", default=None,
                        help="ingestor class name (default: auto-detect)")
    parser.add_argument("--output-dir", dest="output_dir", default=None,
                        help="directory for the parsed packet and thumbnails")
    parser.add_argument("--push", action="store_true",
                        help="push the parsed packet to Crucible instead of only writing it locally")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    output_dir = args.output_dir or os.path.join(os.getcwd(), "dry_run_output", str(args.dsid))
    os.makedirs(output_dir, exist_ok=True)

    # parse
    packet = parse(args.file, args.dsid, args.ingestor)
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
