"""
Unit tests for scope_foundry_ingestors.py
Asserts intended behavior for parsing Molecular Foundry HDF5 files.
"""

import pytest
from unittest.mock import MagicMock, patch
import numpy as np

# Mock matplotlib and PIL before importing ingestors
import sys
sys.modules["matplotlib"] = MagicMock()
sys.modules["matplotlib.pyplot"] = MagicMock()
sys.modules["PIL"] = MagicMock()

# Patch secrets before importing ingestors
patcher_secret = patch("utils.get_secret", return_value="fake_api_key")
patcher_secret.start()

from ingestors.scope_foundry_ingestors import (
    check_orcid_entry,
    ScopeFoundryH5Ingestor,
    SpinBotIngestor
)


# =====================================================================
# check_orcid_entry
# =====================================================================
class TestCheckOrcidEntry:
    def test_valid_orcid(self):
        assert check_orcid_entry("0000-0001-2345-6789") == "0000-0001-2345-6789"
        assert check_orcid_entry("0000-0002-1825-009X") == "0000-0002-1825-009X"

    def test_invalid_orcid(self):
        assert check_orcid_entry("1234") is None
        assert check_orcid_entry("0000-0001-2345-678") is None
        assert check_orcid_entry("0000-0001-2345-678Y") is None

    def test_handles_whitespace(self):
        assert check_orcid_entry("  0000-0001-2345-6789  ") == "0000-0001-2345-6789"

    def test_handles_non_strings(self):
        assert check_orcid_entry(None) is None
        assert check_orcid_entry(1234) is None


# =====================================================================
# ScopeFoundryH5Ingestor
# =====================================================================

@pytest.fixture
def mock_h5():
    """Returns a fake h5py File object."""
    h5 = MagicMock()
    h5.attrs = {"time_id": 1700000000.0}
    return h5


@pytest.fixture
def base_ig():
    """Returns a fresh ScopeFoundryH5Ingestor with basic mock data."""
    ig = ScopeFoundryH5Ingestor()
    ig.file_to_upload = "/path/to/my_data_picam_readout.h5"
    ig.scientific_metadata = {
        "app": {"name": "TestInstrument", "settings": {"save_dir": "/mock/dir"}},
        "hardware": {"mf-crucible": {"settings": {
            "tags": "tag1, tag2",
            "session_name": "session_001",
            "orcid": "0000-0001-2345-6789",
            "proposal": "PROJ-123 name"
        }}}
    }
    ig.keywords = []
    return ig


class TestScopeFoundryBase:
    def test_is_file_supported(self, base_ig):
        base_ig.supported_measurements = ['picam_readout']
        base_ig.file_to_upload = "data_picam_readout.h5"
        assert base_ig.is_file_supported()

        base_ig.file_to_upload = "data_unknown.h5"
        assert not base_ig.is_file_supported()

        # Must end in .h5
        base_ig.file_to_upload = "data_picam_readout.csv"
        assert not base_ig.is_file_supported()

    def test_find_measurement(self, base_ig):
        # Should match exact structure measurement/name
        assert base_ig._find_measurement("measurement/picam") == "picam"
        # Should ignore deeper paths
        assert base_ig._find_measurement("measurement/picam/data") is None

    @patch("ingestors.h5_ingestor.H5Ingestor.get_dataset_metadata")
    def test_get_dataset_metadata_extracts_base_info(self, mock_super, base_ig, mock_h5):
        base_ig.h5file = mock_h5
        base_ig.h5file.visit.return_value = "picam_readout"
        
        base_ig.get_dataset_metadata()
        
        assert base_ig.instrument_name == "TestInstrument"
        assert base_ig.source_folder == "/mock/dir"
        assert base_ig.measurement == "picam_readout"
        assert base_ig.data_format == "ScopeFoundryH5"
        assert "T" in base_ig.timestamp  # ISO format check
        mock_super.assert_called_once()

    @patch("ingestors.h5_ingestor.H5Ingestor.get_dataset_metadata")
    def test_get_dataset_metadata_overwrites_unique_id(self, mock_super, base_ig, mock_h5):
        base_ig.h5file = mock_h5
        base_ig.h5file.attrs["unique_id"] = "h5_internal_id_001"
        
        base_ig.get_dataset_metadata()
        assert base_ig.unique_id == "h5_internal_id_001"

    @patch("ingestors.h5_ingestor.H5Ingestor.get_dataset_metadata")
    def test_get_dataset_metadata_extracts_tags_and_session(self, mock_super, base_ig, mock_h5):
        base_ig.h5file = mock_h5
        base_ig.get_dataset_metadata()
        
        assert "tag1" in base_ig.keywords
        assert "tag2" in base_ig.keywords
        assert base_ig.session_name == "session_001"
        assert "session_001" in base_ig.keywords

    @patch("ingestors.h5_ingestor.H5Ingestor.get_dataset_metadata")
    def test_get_dataset_metadata_handles_missing_hardware_block(self, mock_super, base_ig, mock_h5):
        """The except Exception block correctly catches KeyError if the 'hardware' dict is missing,
        falling back to default tags."""
        base_ig.h5file = mock_h5
        del base_ig.scientific_metadata["hardware"]
        
        base_ig.get_dataset_metadata()
        
        # Falls back gracefully, default values are NOT added to keywords
        assert len(base_ig.keywords) == 0
        assert base_ig.session_name is None

    @pytest.mark.xfail
    def test_parse_orcid(self, base_ig):
        base_ig.owner_orcid = None
        base_ig.parse_orcid()
        assert base_ig.owner_orcid == "0000-0001-2345-6789"

    @pytest.mark.xfail
    def test_parse_project_id(self, base_ig):
        base_ig.project_id = None
        base_ig.parse_project_id()
        assert base_ig.project_id == "PROJ-123"


# =====================================================================
# SpinBotIngestor
# =====================================================================

@pytest.fixture
def spinbot_ig():
    ig = SpinBotIngestor()
    ig.file_to_upload = "/mnt/gcs/campaign_alpha/batch_02/data.h5"
    ig.scientific_metadata = {
        "app": {"name": "SpinBot", "settings": {"save_dir": "/mock", "sample": "SAMP123"}},
        "hardware": {"mf_crucible_spinbot": {"settings": {
            "tags": "spin, bot",
            "session_name": "spin_session",
            "orcid": "0000-0001-2345-6789",
            "proposal": "PROJ_SB",
            "batch_id": "PREFIX_BatchName_UUID12345_SUFFIX"
        }}}
    }
    ig.keywords = []
    ig.samples = []
    ig.batch = {}
    return ig


class TestSpinBotIngestor:
    @patch("ingestors.scope_foundry_ingestors.ScopeFoundryH5Ingestor.get_dataset_metadata")
    def test_get_dataset_metadata(self, mock_super, spinbot_ig):
        spinbot_ig.get_dataset_metadata()
        
        assert "spin" in spinbot_ig.keywords
        assert "bot" in spinbot_ig.keywords
        assert spinbot_ig.session_name == "spin_session"
        assert "spin_session" in spinbot_ig.keywords
        
        # Should extract campaign/batch strings from the filepath
        assert "campaign_alpha" in spinbot_ig.keywords
        assert "batch_02" in spinbot_ig.keywords

    def test_parse_batch(self, spinbot_ig):
        result = spinbot_ig.parse_batch()
        
        # Expected to split 'PREFIX_BatchName_UUID12345_SUFFIX' by '_'
        assert result["unique_id"] == "UUID12345"
        assert result["sample_name"] == "BatchName"
        assert result["owner_orcid"] == "0000-0001-2345-6789"
        
        assert spinbot_ig.batch == result

    def test_parse_samples(self, spinbot_ig):
        spinbot_ig.batch = {"unique_id": "BATCH_ID"}
        # 26 alphanumeric chars
        spinbot_ig.scientific_metadata["app"]["settings"]["sample"] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        spinbot_ig.parse_samples()
        
        assert len(spinbot_ig.samples) == 1
        sample = spinbot_ig.samples[0]
        assert sample["unique_id"] == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        assert sample["sample_name"] == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        assert sample["owner_orcid"] == "0000-0001-2345-6789"
        assert sample["parents"] == [{"unique_id": "BATCH_ID"}]

    def test_parse_samples_handles_invalid_id(self, spinbot_ig):
        spinbot_ig.batch = {"unique_id": "BATCH_ID"}
        # Not 26 chars
        spinbot_ig.scientific_metadata["app"]["settings"]["sample"] = "ShortName"
        
        spinbot_ig.parse_samples()
        
        sample = spinbot_ig.samples[0]
        # ID should be None if it's not a valid 26-char hash
        assert sample["unique_id"] is None
        assert sample["sample_name"] == "ShortName"


# =====================================================================
# Phase 2: Simple Extractors
# =====================================================================

from ingestors.scope_foundry_ingestors import (
    SimpleTiledImageScopeFoundryH5Ingestor,
    CanonCaptureScopeFoundryH5Ingestor,
    SingleSpecScopeFoundryH5Ingestor,
    HyperspecScopeFoundryH5Ingestor,
    HyperspecSweepScopeFoundryH5Ingestor,
    ToupcamLiveScopeFoundryH5Ingestor
)

class TestSimpleTiledImageIngestor:
    @patch("os.listdir", return_value=["img1.jpg", "img2.jpg"])
    def test_get_data_files(self, mock_listdir):
        ig = SimpleTiledImageScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/data.h5"
        ig.associated_files = {}
        
        ig.add_file = MagicMock()
        
        ig.get_data_files()
        
        mock_listdir.assert_called_once_with("/path/data.h5_images")
        ig.add_file.assert_any_call("/path/data.h5_images/img1.jpg")
        ig.add_file.assert_any_call("/path/data.h5_images/img2.jpg")


class TestCanonCaptureIngestor:
    @patch("PIL.Image.open")
    def test_get_thumbnails(self, mock_open):
        ig = CanonCaptureScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/photo.h5"
        ig.thumbnails = []
        
        ig.add_thumbnail = MagicMock()
        
        ig.get_thumbnails()
        
        mock_open.assert_called_once_with("/path/photo.h5.JPG")
        ig.add_thumbnail.assert_called_once_with(mock_open.return_value, "Canon Camera Capture")

    def test_get_data_files(self):
        ig = CanonCaptureScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/photo.h5"
        ig.add_file = MagicMock()
        
        ig.get_data_files()
        ig.add_file.assert_called_once_with("/path/photo.h5.JPG")


class TestSingleSpecIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.plot")
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails(self, mock_open, mock_savefig, mock_plot, mock_h5):
        ig = SingleSpecScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/scan.h5"
        ig.measurement = "picam_readout"
        ig.add_thumbnail = MagicMock()
        
        # Setup mock HDF5 structure
        mock_file = MagicMock()
        mock_group = {
            "spectrum": np.array([1, 2, 3]),
            "raman_shifts": np.array([100, 200, 300])
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        mock_plot.assert_called_once()
        mock_savefig.assert_called_once_with("./generated_files/scan.h5.spectra_plot.jpg")
        ig.add_thumbnail.assert_called_once_with(mock_open.return_value, "Picam Readout")


class TestHyperspecIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.imsave")
    @patch("ingestors.scope_foundry_ingestors.plt.plot")
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails_success(self, mock_open, mock_savefig, mock_plot, mock_imsave, mock_h5):
        ig = HyperspecScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/hyperspec.h5"
        ig.measurement = "m4_hyperspectral_2d_scan"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            # [0] indexing occurs, so we provide an extra dimension
            "spec_map": np.ones((1, 10, 10, 5)),
            "wls": np.array([400, 500, 600, 700, 800])
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        mock_imsave.assert_called_once()
        mock_plot.assert_called_once()
        mock_savefig.assert_called_once()
        assert ig.add_thumbnail.call_count == 2
        ig.add_thumbnail.assert_any_call(mock_open.return_value, "Spectral Map")
        ig.add_thumbnail.assert_any_call(mock_open.return_value, "Sum of Spectra")

    @pytest.mark.xfail
    @patch("h5py.File")
    def test_get_thumbnails_logs_error_on_missing_data(self, mock_h5):
        ig = HyperspecScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/corrupt.h5"
        ig.thumbnails = []
        
        mock_h5.side_effect = Exception("Corrupted HDF5")
        
        with pytest.raises(Exception):
            ig.get_thumbnails()


class TestHyperspecSweepIngestor:
    def test_get_thumbnails_passes(self):
        ig = HyperspecSweepScopeFoundryH5Ingestor()
        ig.thumbnails = []
        ig.get_thumbnails()  # Just passes silently
        assert len(ig.thumbnails) == 0


class TestToupcamLiveIngestor:
    @patch("h5py.File")
    @patch("PIL.Image.fromarray")
    def test_get_thumbnails_finds_image(self, mock_fromarray, mock_h5):
        ig = ToupcamLiveScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/live.h5"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {"image": np.ones((100, 100))}
        # Map ['measurement']['toupcam_live'] to mock_group
        mock_file.__getitem__.return_value.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        mock_fromarray.assert_called_once()
        ig.add_thumbnail.assert_called_once_with(mock_fromarray.return_value, "Toupcam Live Image")

    @patch("h5py.File")
    def test_get_thumbnails_returns_when_no_image(self, mock_h5):
        ig = ToupcamLiveScopeFoundryH5Ingestor()
        ig.file_to_upload = "/path/live.h5"
        ig.thumbnails = []
        
        mock_file = MagicMock()
        mock_group = {"other_key": "data"} # Missing 'image'
        mock_file.__getitem__.return_value.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        assert len(ig.thumbnails) == 0


# =====================================================================
# Phase 3: Complex Multi-Plot Extractors
# =====================================================================

from ingestors.scope_foundry_ingestors import (
    QSpleemSVRampIngestor,
    QSpleemARRESEKIngestor,
    QSpleemARRESMMIngestor,
    NirvanaMultiPosLineScanIngestor
)

class TestQSpleemSVRampIngestor:
    def test_is_file_supported(self):
        ig = QSpleemSVRampIngestor()
        ig.file_to_upload = "/path/data_sv_ramp.h5"
        assert ig.is_file_supported()
        
        ig.file_to_upload = "/path/data_sv_ramp.csv"
        assert not ig.is_file_supported()

    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.plot")
    @patch("ingestors.scope_foundry_ingestors.plt.imshow")
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails_generates_all_plots(self, mock_open, mock_savefig, mock_imshow, mock_plot, mock_h5):
        ig = QSpleemSVRampIngestor()
        ig.file_to_upload = "/path/qspleem.h5"
        ig.measurement = "sv_ramp"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            "0000_sv_array": np.array([1, 2, 3]),
            "000_imavg_array": np.array([10, 20, 15]), # Max is at index 1
            "000_im_array": np.ones((3, 10, 10)),      # 3D array for diffpeak
            "000_im_up_array": np.ones((10, 10)),      # 2D array for basic image
            "000_im_down_array": np.ones((10, 10))     # 2D array for basic image
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        # 1 average plot, 1 diff peak plot, 2 basic images
        assert mock_plot.call_count == 1
        assert mock_imshow.call_count == 3
        assert mock_savefig.call_count == 4
        assert ig.add_thumbnail.call_count == 4


class TestQSpleemARRESMMIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.subplots", return_value=(MagicMock(), MagicMock()))
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails_momentum_map(self, mock_open, mock_savefig, mock_subplots, mock_h5):
        ig = QSpleemARRESMMIngestor()
        ig.file_to_upload = "/path/arres_MM.h5"
        ig.measurement = "ARRES_MM"
        ig.dataset_name = "test_arres"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        # Shape: 2 spectra, 10x10 maps
        mock_group = {
            "spectrum": np.ones((2, 10, 10)),
            "kx": np.array([1, 2]),
            "ky": np.array([1, 2]),
            "settings": MagicMock(attrs={"E": 100})
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        ax_mock = mock_subplots.return_value[1]
        assert ax_mock.imshow.call_count == 2
        assert mock_savefig.call_count == 2
        assert ig.add_thumbnail.call_count == 2


class TestNirvanaMultiPosLineScanIngestor:
    def test_is_file_supported(self):
        ig = NirvanaMultiPosLineScanIngestor()
        ig.file_to_upload = "scan_pollux_oospec_multipos_line_scan_v1.h5"
        assert ig.is_file_supported()
        
        ig.file_to_upload = "scan_pollux_oospec_multipos_line_scan_v1.txt"
        assert not ig.is_file_supported()
        
        ig.file_to_upload = "other_scan.h5"
        assert not ig.is_file_supported()

    @patch("ingestors.h5_ingestor.H5Ingestor.get_dataset_metadata")
    def test_get_dataset_metadata(self, mock_super):
        ig = NirvanaMultiPosLineScanIngestor()
        ig.file_to_upload = "/path/scan.h5"
        ig.keywords = []
        ig.scientific_metadata = {
            "app": {"settings": {"save_dir": "/mock/dir"}},
            "hardware": {"mf_crucible_nirvana": {"settings": {
                "tags": "nirvana, test",
                "session_name": "session_n",
                "orcid": "0000-0000-0000-0000",
                "project": "PROJ-123 name"
            }}}
        }
        
        # Mock h5file traversal
        mock_file = MagicMock()
        mock_file.visit.return_value = "pollux_oospec_multipos_line_scan"
        mock_file.attrs = {"time_id": 1700000000.0, "unique_id": "NIRVANA_001"}
        ig.h5file = mock_file
        
        ig.get_dataset_metadata()
        
        assert ig.instrument_name == "Nirvana Spectrometer"
        assert ig.source_folder == "/mock/dir"
        assert ig.measurement == "pollux_oospec_multipos_line_scan"
        assert ig.unique_id == "NIRVANA_001"
        assert "nirvana" in ig.keywords
        assert "test" in ig.keywords
        assert ig.session_name == "session_n"

    def test_parse_samples(self):
        ig = NirvanaMultiPosLineScanIngestor()
        ig.h5file = MagicMock()
        ig.owner_orcid = "0000-0000-0000-0000"
        ig.project_id = "PROJ-1"
        ig.samples = []
        
        # Mock h5file structure for samples
        pos1 = MagicMock(attrs={"sample_uuid": "SAMP_1", "sample_name": "Sample One"})
        pos2 = MagicMock(attrs={"sample_uuid": "SAMP_2", "sample_name": "Sample Two"})
        mock_positions = {"pos001": pos1, "pos002": pos2}
        
        ig.h5file.__getitem__.return_value = mock_positions
        
        ig.parse_samples()
        
        assert len(ig.samples) == 2
        assert ig.samples[0]["unique_id"] == "SAMP_1"
        assert ig.samples[0]["sample_name"] == "Sample One"
        assert ig.samples[1]["unique_id"] == "SAMP_2"


# =====================================================================
# Phase 4: Remaining Specialized Extractors
# =====================================================================

from ingestors.scope_foundry_ingestors import (
    CLSyncRasterScanIngestor,
    CLHyperspecIngestor,
    SpinbotSpecLineIngestor,
    SpinbotSpecRunIngestor,
    SpinbotCameraCaptureIngestor,
    BioGlowIngestor,
    QSpleemImageIngestor,
    QSpleemARRESEKIngestor
)

class TestCLSyncRasterScanIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.imsave")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails(self, mock_open, mock_imsave, mock_h5):
        ig = CLSyncRasterScanIngestor()
        ig.file_to_upload = "/path/raster.h5"
        ig.measurement = "sync_raster_scan"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            "adc_map": np.ones((1, 1, 10, 10, 2)), # 2 ADC channels
            "ctr_map": np.ones((1, 1, 10, 10, 3))  # 3 Counter channels
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        assert mock_imsave.call_count == 5
        assert ig.add_thumbnail.call_count == 5


class TestCLHyperspecIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.CLSyncRasterScanIngestor.get_thumbnails")
    @patch("ingestors.scope_foundry_ingestors.plt.imsave")
    @patch("ingestors.scope_foundry_ingestors.plt.plot")
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails(self, mock_open, mock_savefig, mock_plot, mock_imsave, mock_super_tn, mock_h5):
        ig = CLHyperspecIngestor()
        ig.file_to_upload = "/path/cl_hyper.h5"
        ig.measurement = "hyperspec_cl"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            "spec_map": np.ones((1, 1, 10, 10, 5)), 
            "wls": np.array([400, 500, 600])
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        mock_super_tn.assert_called_once()
        assert mock_imsave.call_count == 1
        assert mock_plot.call_count == 1
        assert mock_savefig.call_count == 1
        assert ig.add_thumbnail.call_count == 2


class TestSpinbotExtractors:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.plot")
    @patch("ingestors.scope_foundry_ingestors.plt.legend")
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_spec_line_thumbnails(self, mock_open, mock_savefig, mock_legend, mock_plot, mock_h5):
        ig = SpinbotSpecLineIngestor()
        ig.file_to_upload = "/path/spinbot_line.h5"
        ig.measurement = "spec_line_scan"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            "spectra": np.ones((2, 100)), # 2 spectra lines
            "wls": np.arange(100)
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        assert mock_plot.call_count == 2
        mock_legend.assert_called_once()
        mock_savefig.assert_called_once()
        ig.add_thumbnail.assert_called_once()

    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.plot")
    @patch("ingestors.scope_foundry_ingestors.plt.legend")
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.plt.clf")
    @patch("ingestors.scope_foundry_ingestors.Image.fromarray")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_spec_run_thumbnails(self, mock_open, mock_fromarray, mock_clf, mock_savefig, mock_legend, mock_plot, mock_h5):
        ig = SpinbotSpecRunIngestor()
        ig.file_to_upload = "/path/spinbot_run.h5"
        ig.measurement = "spec_run"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            "test_spectra": np.ones((1, 100)),
            "test_wls": np.arange(100),
            "photo": np.ones((100, 100))
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        assert mock_plot.call_count == 1
        assert mock_savefig.call_count == 1
        mock_fromarray.assert_called_once()
        assert ig.add_thumbnail.call_count == 2

    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_camera_capture_thumbnails(self, mock_open):
        ig = SpinbotCameraCaptureIngestor()
        ig.file_to_upload = "/path/spinbot_cam.h5"
        ig.add_thumbnail = MagicMock()
        ig.get_thumbnails()
        
        assert mock_open.call_count == 2
        assert ig.add_thumbnail.call_count == 2
        
    def test_camera_capture_data_files(self):
        ig = SpinbotCameraCaptureIngestor()
        ig.file_to_upload = "/path/spinbot_cam.h5"
        ig.add_file = MagicMock()
        ig.get_data_files()
        
        assert ig.add_file.call_count == 2


class TestBioGlowIngestor:
    def test_is_file_supported(self):
        ig = BioGlowIngestor()
        ig.file_to_upload = "/path/data_bioglow_spec.h5"
        assert ig.is_file_supported()
        ig.file_to_upload = "/path/data.h5"
        assert not ig.is_file_supported()

    @patch("ingestors.scope_foundry_ingestors.run_shell")
    def test_get_data_files(self, mock_run_shell):
        ig = BioGlowIngestor()
        ig.file_to_upload = "/path/data.h5"
        ig.unique_id = "BIO_123"
        ig.add_file = MagicMock()
        
        mock_run_shell.return_value = MagicMock(stdout="zipped", stderr="")
        
        ig.get_data_files()
        
        mock_run_shell.assert_called_once()
        ig.add_file.assert_called_once_with("./generated_files/BIO_123_bioglow_spec_blocks.zip")


class TestQSpleemImageIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.imsave")
    @patch("ingestors.scope_foundry_ingestors.plt.clf")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails(self, mock_open, mock_clf, mock_imsave, mock_h5):
        ig = QSpleemImageIngestor()
        ig.file_to_upload = "/path/qspleem_image.h5"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {"test_im_array": np.ones((10, 10))}
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        mock_imsave.assert_called_once()
        mock_clf.assert_called_once()
        ig.add_thumbnail.assert_called_once()


class TestQSpleemARRESEKIngestor:
    @patch("h5py.File")
    @patch("ingestors.scope_foundry_ingestors.plt.subplots", return_value=(MagicMock(), MagicMock()))
    @patch("ingestors.scope_foundry_ingestors.plt.savefig")
    @patch("ingestors.scope_foundry_ingestors.plt.clf")
    @patch("ingestors.scope_foundry_ingestors.Image.open")
    def test_get_thumbnails(self, mock_open, mock_clf, mock_savefig, mock_subplots, mock_h5):
        ig = QSpleemARRESEKIngestor()
        ig.file_to_upload = "/path/qspleem_EK.h5"
        ig.dataset_name = "ds"
        ig.measurement = "ARRES_EK"
        ig.add_thumbnail = MagicMock()
        
        mock_file = MagicMock()
        mock_group = {
            "spectrum": np.ones((1, 10, 10)),
            "eV": np.arange(10),
            "uv": np.ones((10, 2))
        }
        mock_file.__getitem__.return_value = mock_group
        mock_h5.return_value.__enter__.return_value = mock_file
        
        ig.get_thumbnails()
        
        assert mock_subplots.call_count == 1
        ax_mock = mock_subplots.return_value[1]
        ax_mock.imshow.assert_called_once()
        mock_savefig.assert_called_once()
        ig.add_thumbnail.assert_called_once()
