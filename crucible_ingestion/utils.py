
import base64
import math
import json
import numpy as np
from io import BytesIO
from PIL import Image
from datetime import datetime


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



