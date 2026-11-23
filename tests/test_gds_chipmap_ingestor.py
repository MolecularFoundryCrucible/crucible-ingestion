"""GdsChipMapIngestor against small synthetic GDS files built with gdstk, fully offline.

The real-mask test skips unless the WFR_04 GDS is placed (not committed) in tests/data/.

    uv run pytest tests/test_gds_chipmap_ingestor.py
"""
import json
from pathlib import Path
from types import SimpleNamespace

import gdstk
import numpy as np
import pytest

from crucible_ingestion.client import set_client
from crucible_ingestion.data_ingestion import parse
from crucible_ingestion.ingestors.gds_chipmap_ingestor import GdsChipMapIngestor, extract_geometry
from crucible_ingestion.ingestors.registry import find_supported_ingestor

REAL_GDS = Path(__file__).parent / "data" / "QCMF_WFR_04_Al_250nm_all layers.gds"


def frame(x0, y0, w, h, ring, layer, datatype=0):
    outer = gdstk.rectangle((x0, y0), (x0 + w, y0 + h))
    inner = gdstk.rectangle((x0 + ring, y0 + ring), (x0 + w - ring, y0 + h - ring))
    (polygon,) = gdstk.boolean(outer, inner, "not", layer=layer, datatype=datatype)
    return polygon


def build_mask(path):
    """Units are µm, like the real masks."""
    lib = gdstk.Library(unit=1e-6, precision=1e-9)

    chip = lib.new_cell("CHIP")
    chip.add(frame(0, 0, 5000, 3000, 20, layer=2))
    chip.add(gdstk.rectangle((100, 100), (200, 200), layer=2))      # 0.1 mm: below the size cut

    text_pcell = lib.new_cell("TEXT$1")
    text_pcell.add(gdstk.rectangle((0, 0), (50, 50), layer=0))
    lib.new_cell("TEXT$9")                                           # lost its PCell context

    context = lib.new_cell("$$$CONTEXT_INFO$$$")
    ref = gdstk.Reference(text_pcell)
    ref.set_gds_property(1, "P(text)='R1D1'")
    context.add(ref)

    top = lib.new_cell("MASK")
    top.add(frame(-10000, -10000, 10000, 10000, 20, layer=1, datatype=10))
    top.add(frame(-10000, -10000, 10000, 10000, 20, layer=1, datatype=10))   # exact duplicate
    top.add(gdstk.Reference(chip, origin=(20000, 0), rotation=np.pi / 2, columns=2, rows=1, spacing=(4000, 0)))
    top.add(gdstk.FlexPath([(0, -5000), (3000, -5000)], 1500, layer=3))
    top.add(gdstk.Reference(text_pcell, origin=(1000, 2000)))
    top.add(gdstk.Reference(lib["TEXT$9"], origin=(-3000, 500)))
    top.add(gdstk.Label("NOTE", (7000, 8000), layer=0))

    lib.new_cell("SCRAP").add(gdstk.rectangle((0, 0), (5, 5), layer=9))       # stray second top cell
    lib.write_gds(str(path))
    return str(path)


@pytest.fixture
def mask(tmp_path):
    return build_mask(tmp_path / "mask.gds")


class FakeDatasets:
    def get(self, dsid, include_metadata=True):
        return None

    def list_files(self, dsid):
        return []


@pytest.fixture
def offline_client():
    set_client(SimpleNamespace(datasets=FakeDatasets()))
    yield
    set_client(None)


def test_claims_gds_by_header_on_opaque_key(mask, tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_bytes(Path(mask).read_bytes())
    assert GdsChipMapIngestor(file_to_upload=str(opaque), file_name="m.gds", unique_id="x").is_file_supported()
    assert find_supported_ingestor(str(opaque), "x", file_name="m.gds")[1] == "GdsChipMapIngestor"


def test_rejects_non_gds_bytes_and_other_extensions(mask, tmp_path):
    fake = tmp_path / "fake.gds"
    fake.write_bytes(b"not a gds file")
    assert not GdsChipMapIngestor(file_to_upload=str(fake), unique_id="x").is_file_supported()
    assert not GdsChipMapIngestor(file_to_upload=mask, file_name="m.oas", unique_id="x").is_file_supported()


def test_shapes_are_flattened_filtered_and_deduplicated(mask):
    g = extract_geometry(mask)
    boxes = [(s["layer"], s["x0"], s["y0"], s["x1"], s["y1"]) for s in g["shapes"]]
    assert boxes == [
        ("1/10", -10.0, -10.0, 0.0, 0.0),                # duplicate removed
        # CHIP rotated 90° at (20, 0) mm, arrayed along x after rotation by 4 mm
        ("2/0", 17.0, 0.0, 20.0, 5.0),
        ("2/0", 17.0, 4.0, 20.0, 9.0),
        ("3/0", 0.0, -5.75, 3.0, -4.25),                  # path converted to a polygon
    ]
    frame_shape = g["shapes"][0]
    assert frame_shape["n_points"] > 4 and frame_shape["fill"] < 0.01
    assert g["shapes"][3]["fill"] == 1.0


def test_labels_and_unresolved_pcells(mask):
    g = extract_geometry(mask)
    assert g["labels"] == [{"text": "NOTE", "x": 7.0, "y": 8.0, "source": "text"},
                           {"text": "R1D1", "x": 1.0, "y": 2.0, "source": "pcell"}]
    assert g["unresolved_labels"] == [{"cell": "TEXT$9", "x": -3.0, "y": 0.5}]


def test_source_describes_file(mask):
    src = extract_geometry(mask)["source"]
    assert src["top_cell"] == "MASK"
    assert src["other_top_cells"] == ["SCRAP"]
    assert src["gds_unit_um"] == 1.0
    assert src["layers"] == ["0/0", "1/10", "2/0", "3/0", "9/0"]
    assert src["polygon_counts"]["2/0"] == 2 and src["polygon_counts"]["1/10"] == 2


def test_output_is_identical_between_runs(mask):
    assert json.dumps(extract_geometry(mask)) == json.dumps(extract_geometry(mask))


def test_full_parse_packet(mask, offline_client):
    packet = parse(mask, "dsid1", "GdsChipMapIngestor", file_name="WFR_99.gds")
    g = packet.scientific_metadata["gds_geometry"]
    assert g["source"]["file"] == "WFR_99.gds"
    assert g["units"] == "mm" and g["min_size_mm"] == 1.0
    assert packet.dataset_fields["measurement"] == "Mask design"
    assert packet.dataset_fields["data_type"] == "GDS layout"
    assert len(packet.thumbnails) == 1
    json.dumps(packet.scientific_metadata, allow_nan=False)


@pytest.mark.skipif(not REAL_GDS.exists(), reason="WFR_04 GDS not present (never committed)")
def test_real_mask_wfr04():
    g = extract_geometry(str(REAL_GDS))
    assert g["source"]["top_cell"] == "Photomask"
    by_layer = {}
    for s in g["shapes"]:
        by_layer[s["layer"]] = by_layer.get(s["layer"], 0) + 1
    assert by_layer["2/0"] == 106 and by_layer["1/10"] == 4 and by_layer["10/0"] == 3
    assert len(g["labels"]) == 57 and len({l["text"] for l in g["labels"]}) == 14
    assert len(g["unresolved_labels"]) == 7
