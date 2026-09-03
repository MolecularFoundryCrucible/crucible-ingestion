
import base64
import logging
import math
import json
import subprocess
import numpy as np
from numbers import Rational
from io import BytesIO
from PIL import Image
from datetime import datetime
from importlib.metadata import distribution
from urllib.parse import urlparse
from urllib.request import url2pathname

logger = logging.getLogger(__name__)


def build_b64_thumbnail(image: Image, max_size = (200,200)):
    image.thumbnail(max_size)
    image = image.convert("RGB")
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    thumbnail = base64.b64encode(buffered.getvalue()).decode("UTF-8")
    return(thumbnail)


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


def sanitize_metadata(obj):
    if isinstance(obj, dict):
        return {k: sanitize_metadata(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_metadata(v) for v in obj]
    if isinstance(obj, tuple):
        return [sanitize_metadata(v) for v in obj]
    # PIL's IFDRational (TIFF resolution tags) and other Rationals are not JSON types
    if isinstance(obj, Rational) and not isinstance(obj, int):
        # TIFF allows a zero denominator, which float() cannot represent
        return None if obj.denominator == 0 else float(obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='replace')
    return obj


def deep_merge_skip_empty(target, source):
    """Recursively merge source into target. Source values win, except None/
    empty-string/'unknown' values, which never overwrite existing target data.
    Nested dicts are merged key-by-key rather than replaced wholesale."""
    for key, src_val in source.items():
        if isinstance(src_val, dict) and isinstance(target.get(key), dict):
            deep_merge_skip_empty(target[key], src_val)
            continue
        if src_val is None or src_val == "" or src_val == "unknown":
            continue
        target[key] = src_val
    return target


def get_ingestion_githash():
    try:
        direct_url = distribution("crucible-ingestion").read_text("direct_url.json")
        dist_info = json.loads(direct_url)
        if dist_info.get('vcs_info') is not None:
            return dist_info["vcs_info"]["commit_id"]
        elif dist_info.get('dir_info') is not None:
            return _git_describe_checkout(url2pathname(urlparse(dist_info["url"]).path))
        else:
            logger.warning("crucible-ingestion has no vcs_info or dir_info; cannot resolve githash")
            return None

    except Exception as err:
        logger.warning(f"Could not resolve crucible-ingestion githash: {err}")
        return None


def _git_describe_checkout(path):
    """Resolve HEAD of the git checkout at path, suffixed '-dirty' if it has uncommitted changes."""
    def git(*args):
        return subprocess.run(('git', '-C', path) + args, capture_output=True,
                              text=True, check=True).stdout.strip()

    commit = git('rev-parse', 'HEAD')
    return f'{commit}-dirty' if git('status', '--porcelain') else commit

