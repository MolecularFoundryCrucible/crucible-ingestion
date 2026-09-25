import io
import re

from pathlib import Path
from PIL import Image
import ncempy.io as nio
import matplotlib.pyplot as plt
import logging
import numpy as np

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


class MrcIngestor(CrucibleDatasetIngestor):
    '''subclass for ingesting MRC files'''

    def is_file_supported(self):
        if not Path(self.file_to_upload).suffix == '.mrc':
            return False
        try:
            with nio.mrc.fileMRC(self.file_to_upload) as mrc1:
                return True
        except Exception as e:
            logger.error(f"Error occurred while checking MRC file support: {e}")
            return False

    
    def get_scientific_metadata(self):
        with nio.mrc.fileMRC(self.file_to_upload) as mrc1:
            md = mrc1.getMetadata()
            for key, value in md.items():
                if isinstance(value, np.ndarray) and not value.flags.c_contiguous:
                    md[key] = list(value)
            self.scientific_metadata.update(md)
        logger.info(f'Got metadata from MRC: {self.scientific_metadata=}')

        file_path = Path(self.file_to_upload)
        # Read tilt angles from .rawtlt file if it exists
        rawtltName = file_path.with_suffix('.rawtlt')
        if rawtltName.exists():
            with open(rawtltName, 'r') as f1:
                tilts = list(map(float, f1))
            self.scientific_metadata['tilt angles'] = tilts
        
        # Read FEI parameters from .txt file if it exists
        FEIparameters = file_path.with_suffix('.txt')
        if FEIparameters.exists():
            try:
                with open(FEIparameters, 'r', encoding='utf-8-sig') as f2:
                    lines = f2.readlines()
            except UnicodeDecodeError:
                with open(FEIparameters, 'r', encoding='cp1252') as f2:
                    lines = f2.readlines()
            self.scientific_metadata['fei_parameters'] = _parse_fei_parameters(lines)

    def parse_measurement(self):
        # Test for metadata that is indicative of a tilt series from FEI tomo software.
        if self.scientific_metadata.get('axisOrientations') is not None and self.scientific_metadata.get('cellAngles') is not None:
            self.measurement = 'tomography'
        else:
            self.measurement = None
        logger.info(f'{self.measurement=}')

    def get_dataset_metadata(self):
         # Use parent class method to set data_format and size
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.dataset_name = Path(self.file_to_upload).name


    def generate_thumbnail(self, target_size=(200, 200), dpi=100):
        """Generate a thumbnail from an MRC image as a PNG.

        Parameters
        ----------
        target_size : tuple
            Desired size of the thumbnail in pixels (width, height).
        dpi : int
            Dots per inch for the thumbnail image.

        Returns
        -------
        : PIL.Image
            Thumbnail image as a PIL Image object.

        """

        fig_size = (target_size[0] / dpi, target_size[1] / dpi) # inches
       
        fg = None
        buf = None
        try:
            with nio.mrc.fileMRC(self.file_to_upload) as mrc1:
                if mrc1.dataSize.shape[0] ==3:
                    image_array = mrc1.getSlice(mrc1.dataSize[0] // 2)  # Get the middle slice for 3D data, or the only slice for 2D data
                else:
                    image_array = mrc1.getSlice(0)  # Get the first slice for 2D data

            if image_array is None:
                raise ValueError("No data found in MRC file.")

            fg, ax = plt.subplots(1, 1, figsize=fig_size, dpi=dpi)
            ax.imshow(image_array, cmap='gray')
            ax.axis('off')
            fg.tight_layout(pad=0.05)

            # Decode the rendered image, then enforce the requested resolution.
            buf = io.BytesIO()
            fg.savefig(buf, bbox_inches='tight', pad_inches=0.05, dpi=dpi)
            buf.seek(0)
            with Image.open(buf) as rendered_image:
                im = rendered_image.resize(target_size, Image.Resampling.LANCZOS)
            return im
        except Exception as e:
            logger.exception("Failed to generate thumbnail: %s", e)
            return None
        finally:
            if buf is not None:
                buf.close()
            if fg is not None:
                plt.close(fg)

    def get_thumbnails(self):
        try:
            thumbnail = self.generate_thumbnail()
            if thumbnail:
                self.add_thumbnail(thumbnail, "MRC_Thumbnail")
        except Exception as e:
            logger.exception("Failed to extract thumbnail: %s", e)