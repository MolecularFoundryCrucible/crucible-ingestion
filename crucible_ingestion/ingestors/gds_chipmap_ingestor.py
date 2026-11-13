import gc
import logging
import re
from io import BytesIO
from pathlib import Path

import gdstk
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import PatchCollection
from matplotlib.patches import Rectangle
from PIL import Image

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

GDS_HEADER = b'\x00\x06\x00\x02'
MIN_SIZE_MM = 1.0
CONTEXT_CELL = '$$$CONTEXT_INFO$$$'
PCELL_TEXT = re.compile(r"P\(text\)='(.*)'")
TEXT_CELL = re.compile(r'^TEXT(\$\d+)?$')
# a layer that cannot exist, so a filtered read loads structure and labels but no polygons
NO_POLYGONS = {(65536, 65536)}


def layer_name(layer, datatype):
    return f'{layer}/{datatype}'


def transform_points(points, ref):
    pts = np.asarray(points, dtype=float).reshape(-1, 2)
    if ref.x_reflection:
        pts = pts * [1, -1]
    if ref.magnification != 1:
        pts = pts * ref.magnification
    if ref.rotation:
        c, s = np.cos(ref.rotation), np.sin(ref.rotation)
        pts = pts @ np.array([[c, s], [-s, c]])
    pts = pts + ref.origin
    offsets = ref.repetition.get_offsets() if ref.repetition.size else np.zeros((1, 2))
    return [pts + offset for offset in offsets]


def transform_box(box, ref):
    x0, y0, x1, y1 = box
    corners = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
    return [(p[:, 0].min(), p[:, 1].min(), p[:, 0].max(), p[:, 1].max())
            for p in transform_points(corners, ref)]


def polygon_offsets(polygon):
    return polygon.repetition.get_offsets() if polygon.repetition.size else np.zeros((1, 2))


def pcell_texts(lib):
    """KLayout TEXT PCell strings, which live on references inside the context cell."""
    texts = {}
    for cell in lib.cells:
        if cell.name != CONTEXT_CELL:
            continue
        for ref in cell.references:
            for prop in ref.properties:
                for value in prop[1:]:
                    s = value.decode(errors='replace').rstrip('\x00') if isinstance(value, bytes) else str(value)
                    match = PCELL_TEXT.match(s)
                    if match:
                        texts[ref.cell.name] = match.group(1)
    return texts


def choose_top_cell(lib):
    tops = [c for c in lib.top_level() if c.name != CONTEXT_CELL]
    if not tops:
        raise ValueError('GDS has no top-level cell')
    # several tops: the one instantiating the most is the design, the rest are leftovers
    def descendants(cell):
        return len(cell.dependencies(True))
    return max(tops, key=lambda c: (descendants(c), c.name)), [c.name for c in tops]


def collect_labels(top, texts):
    """Plain labels and PCell text labels, flattened into top-cell coordinates."""
    memo = {}

    def walk(cell):
        if cell.name in memo:
            return memo[cell.name]
        found = [(label.text, label.origin[0], label.origin[1], 'text') for label in cell.labels]
        for ref in cell.references:
            name = ref.cell.name
            if name in texts:
                items = [(texts[name], 0.0, 0.0, 'pcell')]
            elif TEXT_CELL.match(name):
                items = [(None, 0.0, 0.0, name)]
            else:
                items = walk(ref.cell)
            for text, x, y, source in items:
                for pts in transform_points([(x, y)], ref):
                    found.append((text, pts[0, 0], pts[0, 1], source))
        memo[cell.name] = found
        return found

    return walk(top)


def collect_boxes(top, min_size, prefilter):
    """(x0, y0, x1, y1, n_points, fill) of every polygon on the loaded layer, flattened."""
    memo = {}

    def walk(cell):
        if cell.name in memo:
            return memo[cell.name]
        found = []
        shapes = list(cell.polygons)
        for path in cell.paths:
            shapes.extend(path.to_polygons())
        for polygon in shapes:
            bbox = polygon.bounding_box()
            if bbox is None:
                continue
            (x0, y0), (x1, y1) = bbox
            w, h = x1 - x0, y1 - y0
            if w < prefilter or h < prefilter:
                continue
            fill = abs(polygon.area()) / (w * h)
            for dx, dy in polygon_offsets(polygon):
                found.append((x0 + dx, y0 + dy, x1 + dx, y1 + dy, len(polygon.points), fill))
        for ref in cell.references:
            for x0, y0, x1, y1, n, fill in walk(ref.cell):
                for box in transform_box((x0, y0, x1, y1), ref):
                    found.append((*box, n, fill))
        memo[cell.name] = found
        return found

    return [b for b in walk(top) if b[2] - b[0] >= min_size and b[3] - b[1] >= min_size]


def extract_geometry(path, min_size_mm=MIN_SIZE_MM):
    info = gdstk.gds_info(path)
    mm_per_unit = info['unit'] * 1e3

    structure = gdstk.read_gds(path, filter=NO_POLYGONS)
    top, tops = choose_top_cell(structure)
    max_mag = max([r.magnification for c in structure.cells for r in c.references] + [1.0])
    texts = pcell_texts(structure)
    raw_labels = collect_labels(top, texts)
    top_name = top.name
    del structure, top
    gc.collect()

    min_size = min_size_mm / mm_per_unit
    shapes, counts = {}, {}
    # one layer at a time keeps peak memory near the largest layer, not the whole file
    for layer, datatype in sorted(info['layers_and_datatypes']):
        lib = gdstk.read_gds(path, filter={(layer, datatype)})
        counts[layer_name(layer, datatype)] = sum(len(c.polygons) for c in lib.cells)
        cell = next(c for c in lib.cells if c.name == top_name)
        for x0, y0, x1, y1, n, fill in collect_boxes(cell, min_size, min_size / max_mag):
            box = tuple(round(v * mm_per_unit, 4) for v in (x0, y0, x1, y1))
            key = (layer, datatype, *box)
            shapes.setdefault(key, {
                'layer': layer_name(layer, datatype),
                'x0': box[0], 'y0': box[1], 'x1': box[2], 'y1': box[3],
                'n_points': n, 'fill': round(min(fill, 1.0), 3),
            })
        del lib, cell
        gc.collect()

    labels, unresolved = [], []
    for text, x, y, source in raw_labels:
        x, y = round(x * mm_per_unit, 4), round(y * mm_per_unit, 4)
        if text is None:
            unresolved.append({'cell': source, 'x': x, 'y': y})
        else:
            labels.append({'text': text, 'x': x, 'y': y, 'source': source})

    return {
        'units': 'mm',
        'source': {
            'top_cell': top_name,
            'other_top_cells': sorted(set(tops) - {top_name}),
            'gds_unit_um': info['unit'] * 1e6,
            'layers': [layer_name(*ld) for ld in sorted(info['layers_and_datatypes'])],
            'polygon_counts': counts,
        },
        'min_size_mm': min_size_mm,
        # sorted on content so re-ingesting the same file yields the same order
        'shapes': [shapes[k] for k in sorted(shapes)],
        'labels': sorted(labels, key=lambda l: (l['text'], l['x'], l['y'])),
        'unresolved_labels': sorted(unresolved, key=lambda l: (l['cell'], l['x'], l['y'])),
    }


class GdsChipMapIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        if not self.match_name.lower().endswith('.gds'):
            return False
        with open(self.file_to_upload, 'rb') as f:
            return f.read(4) == GDS_HEADER

    def parse_measurement(self):
        self.measurement = 'Mask design'

    def parse_data_type(self):
        self.data_type = 'GDS layout'

    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        geometry = extract_geometry(self.file_to_upload)
        geometry['source'] = {'file': Path(self.match_name).name, **geometry['source']}
        self.scientific_metadata['gds_geometry'] = geometry

    def get_thumbnails(self):
        self.thumbnails = []
        geometry = self.scientific_metadata.get('gds_geometry', {})
        shapes = geometry.get('shapes', [])
        if not shapes:
            return
        try:
            layers = sorted({s['layer'] for s in shapes})
            colors = {layer: plt.cm.tab10(i % 10) for i, layer in enumerate(layers)}
            fig, ax = plt.subplots(figsize=(8, 8))
            for layer in layers:
                patches = [Rectangle((s['x0'], s['y0']), s['x1'] - s['x0'], s['y1'] - s['y0'])
                           for s in shapes if s['layer'] == layer]
                ax.add_collection(PatchCollection(patches, facecolor='none', edgecolor=colors[layer],
                                                  linewidth=0.6, label=layer))
            for label in geometry.get('labels', []):
                ax.text(label['x'], label['y'], label['text'], fontsize=4)
            ax.autoscale_view()
            ax.set_aspect('equal')
            ax.set_xlabel('x (mm)')
            ax.set_ylabel('y (mm)')
            ax.legend(handles=[Rectangle((0, 0), 1, 1, facecolor='none', edgecolor=colors[l]) for l in layers],
                      labels=layers, fontsize=6, loc='upper right')
            buf = BytesIO()
            fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'GDS outline', size=(800, 800))
        except Exception as err:
            logger.error(f'Failed to generate GDS outline thumbnail: {err}')
