"""ScopeFoundry ingestors claim files by the measurement written inside the H5.

The filename is not authoritative: ScopeFoundry apps append modifiers to it
('_laseroff' backgrounds, rerun counters, copies), while the measurement group
under measurement/ is what parse_measurement records. These tests build minimal
ScopeFoundry files and check claiming through the measurement_aliases mechanism,
including the _sweep collision that naive prefix matching would cause.
"""

import h5py
import numpy as np

from crucible_ingestion.ingestors.registry import find_supported_ingestor
from crucible_ingestion.ingestors.scope_foundry_ingestors import (
    HyperspecScopeFoundryH5Ingestor,
    HyperspecSweepScopeFoundryH5Ingestor,
)


def scopefoundry_file(tmp_path, measurement_name):
    path = tmp_path / f"260804_212901_{measurement_name}.h5"
    with h5py.File(path, 'w') as h5file:
        h5file.attrs['time_id'] = 1785904141.0
        h5file.attrs['unique_id'] = '0tkjqg6ggdvpn000ebydwf7tj4'
        app = h5file.create_group('app')
        app.attrs['name'] = 'HiP_Microscope'
        meas = h5file.create_group(f"measurement/{measurement_name}")
        meas.create_dataset('spec_map', data=np.zeros((1, 2, 2, 4)))
        meas.create_dataset('wls', data=np.arange(4, dtype=float))
    return str(path)


def ingestor_of(cls, path):
    return cls(file_to_upload=path, unique_id='xxx')


def test_measurement_variant_is_claimed_by_content(tmp_path):
    path = scopefoundry_file(tmp_path, 'hyperspec_picam_mcl_laseroff')
    assert ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()


def test_canonical_measurement_still_claimed(tmp_path):
    path = scopefoundry_file(tmp_path, 'hyperspec_picam_mcl')
    assert ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()


def test_sweep_variant_is_not_stolen_by_prefix(tmp_path):
    path = scopefoundry_file(tmp_path, 'hyperspec_picam_mcl_sweep')
    assert not ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()
    assert ingestor_of(HyperspecSweepScopeFoundryH5Ingestor, path).is_file_supported()


from crucible_ingestion.ingestors.scope_foundry_ingestors import (
    QSpleemSVRampIngestor,
    QSpleemARRESEKIngestor,
    BioGlowIngestor,
)


def test_qspleem_sv_ramp_claims_opaque_path_via_file_name(tmp_path):
    path = scopefoundry_file(tmp_path, 'sv_ramp')
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_bytes(open(path, 'rb').read())
    ig = QSpleemSVRampIngestor(file_to_upload=str(opaque), file_name="run_sv_ramp.h5", unique_id="x")
    assert ig.is_file_supported() is True


def test_qspleem_arres_ek_regex_matches_against_file_name_not_opaque_path(tmp_path):
    path = scopefoundry_file(tmp_path, 'ARRES_EK')
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_bytes(open(path, 'rb').read())
    ig = QSpleemARRESEKIngestor(file_to_upload=str(opaque), file_name="run_ARRES_EK_images_0000.h5", unique_id="x")
    assert ig.is_file_supported() is True
    ig_no_name = QSpleemARRESEKIngestor(file_to_upload=str(opaque), unique_id="x")
    assert ig_no_name.is_file_supported() is False


def test_bioglow_still_claims_legacy_path_without_file_name(tmp_path):
    path = scopefoundry_file(tmp_path, 'bioglow_spec')
    legacy = tmp_path / "run_bioglow_spec.h5"
    legacy.write_bytes(open(path, 'rb').read())
    ig = BioGlowIngestor(file_to_upload=str(legacy), unique_id="x")
    assert ig.is_file_supported() is True


def test_unrelated_measurement_not_claimed(tmp_path):
    path = scopefoundry_file(tmp_path, 'toupcam_live')
    assert not ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()


def test_file_without_measurement_group_not_claimed(tmp_path):
    path = str(tmp_path / "no_measurement_group.h5")
    with h5py.File(path, 'w') as h5file:
        h5file.create_group('app')
    assert not ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()


def test_corrupt_file_not_claimed(tmp_path):
    path = str(tmp_path / "garbage.h5")
    with open(path, 'wb') as f:
        f.write(b"not an h5 file")
    assert not ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()


def test_non_h5_not_claimed(tmp_path):
    path = str(tmp_path / "something.txt")
    with open(path, 'w') as f:
        f.write("plain text")
    assert not ingestor_of(HyperspecScopeFoundryH5Ingestor, path).is_file_supported()


def test_parse_measurement_records_group_name(tmp_path):
    """The dataset records the actual variant, not the canonical alias target."""
    path = scopefoundry_file(tmp_path, 'hyperspec_picam_mcl_laseroff')
    ig = ingestor_of(HyperspecScopeFoundryH5Ingestor, path)
    ig.parse_measurement()
    assert ig.measurement == 'hyperspec_picam_mcl_laseroff'


def test_registry_claims_laseroff_file_with_hyperspec_ingestor(tmp_path):
    path = scopefoundry_file(tmp_path, 'hyperspec_picam_mcl_laseroff')
    ig, cls = find_supported_ingestor(path, 'xxx')
    assert cls == 'HyperspecScopeFoundryH5Ingestor'
