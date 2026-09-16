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
