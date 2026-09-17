import re
from datetime import datetime as dt
from typing import ClassVar

import logging
import numpy as np

from crucible_ingestion.ingestors.mrc_ingestor import _parse_fei_parameters

from .crucible_ingestor import CrucibleDatasetIngestor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Every line is prefixed with a fixed-width "MM/DD/YY HH:MM:SS " timestamp; what follows
# it is indented to show which section a parameter belongs to.
_TIMESTAMP_RE = re.compile(r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ')


def _parse_fei_value(raw):
    raw = raw.strip()
    if raw == '':
        return None
    if raw in ('Yes', 'ON'):
        return True
    if raw in ('No', 'OFF'):
        return False
    try:
        return float(raw)
    except ValueError:
        return raw


_VALUE_KEY = '_value'


def _read_fei_lines(file_path):
    """These files contian a degree symbol that is not UTF-8 compatible,
    so we try to read it as UTF-8 first, and if that fails, we read it as cp1252."""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as file:
            return file.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='cp1252') as file:
            return file.readlines()


def _parse_fei_parameters(lines):
    """Parse the vendor tomography-parameter log into a nested dict.

    Section headers (e.g. "STEM imaging mode", "Check Focus") repeat parameter names
    like "Periodicity (high tilt range)" under different settings, so a flat dict would
    have later sections silently overwrite earlier ones. Indentation depth tells sections
    apart from their children, so it is used to nest rather than flatten them.

    A line can be a leaf, a header with no value of its own ("STEM imaging mode"), or
    both at once ("Check Focus: Yes" has its own value and also has Periodicity settings
    indented beneath it) -- every line is therefore pushed as a potential parent, and
    _collapse resolves what it actually turned out to be once all its children are known.
    """
    root = {}
    stack = [(-1, root)]
    for raw_line in lines:
        line = _TIMESTAMP_RE.sub('', raw_line)
        stripped = line.strip()
        if not stripped:
            continue

        # The stack must unwind to this line's depth before deciding whether to skip it,
        # or a skipped section header (e.g. a "-----" rule right after a depth-1 line)
        # would leave a stale frame on the stack and misparent everything that follows.
        depth = len(line) - len(line.lstrip(' '))
        while stack[-1][0] >= depth:
            stack.pop()

        if set(stripped) == {'-'}:
            continue  # decorative rule; never has children of its own
        parent = stack[-1][1]

        if ':' in stripped:
            key, _, value = stripped.partition(':')
            key, value = key.strip(), _parse_fei_value(value)
        else:
            key, value = stripped, None

        node = {_VALUE_KEY: value}
        parent[key] = node
        stack.append((depth, node))

    _collapse(root)
    return root


def _collapse(node):
    """Resolve each {_value, ...children} node into its final shape.

    No children and no value -> True (a bare flag like "STEM imaging mode" turned out
    to introduce no sub-parameters). No children, a value -> that value. Children and no
    value -> a dict of just the children. Both -> a dict of the children plus 'value'.
    """
    for key, child in node.items():
        value = child.pop(_VALUE_KEY)
        _collapse(child)
        if not child:
            node[key] = value if value is not None else True
        elif value is not None:
            child['value'] = value
            node[key] = child
        else:
            node[key] = child

class MrcTxtIngestor(CrucibleDatasetIngestor):
    '''subclass for ingesting txt files associated with FEI automated
    tomography MRC files.'''

    supported_filetypes: ClassVar[list[str]] = ['txt']

    def is_file_supported(self):
        has_ending = np.any([self.file_to_upload.endswith(ftype)
                       for ftype in self.supported_filetypes])

        lines = _read_fei_lines(self.file_to_upload)
        first_line, second_line = lines[:2]

        has_header = 'Date/Time:' in first_line
        has_slashes = '---------------' in second_line

        if has_ending and has_header and has_slashes:
            return True

    def get_scientific_metadata(self):
        """Extract scientific metadata from the ser file using ncempy."""
        super().get_scientific_metadata()

        lines = _read_fei_lines(self.file_to_upload)
        self.scientific_metadata['fei_parameters'] = _parse_fei_parameters(lines)

    def get_dataset_metadata(self):
        super().get_dataset_metadata()

        acquired_date = self.scientific_metadata.get('fei_parameters', {}).get('Date/Time')
        if acquired_date:
            self.timestamp = dt.strptime(acquired_date, '%m/%d/%y %H:%M:%S').isoformat()
