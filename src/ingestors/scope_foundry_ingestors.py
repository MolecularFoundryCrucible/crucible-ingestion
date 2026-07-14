import os
import re
import functools
import logging
from io import BytesIO
from typing import ClassVar

import h5py
import numpy as np
from datetime import datetime
from PIL import Image
import matplotlib.pyplot as plt
from .h5_ingestor import H5Ingestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def check_orcid_entry(orcid_string):
    """Validate and return an ORCID string if it matches the expected format."""
    if not isinstance(orcid_string, str):
        return None
    orcid_string = orcid_string.strip()
    if re.match(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$', orcid_string):
        return orcid_string
    return None

class ScopeFoundryH5Ingestor(H5Ingestor):
    supported_measurements: ClassVar[list[str]] = ['simple_tiled_image', 
                                                    'canon_camera_capture', 
                                                    'picam_readout',
                                                    'm4_hyperspectral_2d_scan',
                                                    'andor_hyperspec_scan',
                                                    'hyperspectral_2d_scan',
                                                    'fiber_winspec_scan',
                                                    'hyperspec_picam_mcl',
                                                    'hyperspec_picam_mcl_sweep',
                                                    'asi_hyperspec_scan',
                                                    'asi_OO_hyperspec_scan',
                                                    'oo_asi_hyperspec_scan',
                                                    'andor_asi_hyperspec_scan', 
                                                    'ald_run_upd',
                                                    'ald_run',
                                                    'ald_run_measure'
                                                    ]
    
    def is_file_supported(self):
        if self.file_to_upload.endswith('h5'):
            return np.any([self.file_to_upload.endswith(f"{meas_name}.h5")
                           for meas_name in self.supported_measurements])
        return False
    
    def parse_measurement(self):
        self.measurement = self.h5file.visit(self._find_measurement)


    def get_dataset_metadata(self):
        self.instrument_name = self.scientific_metadata['app']['name']
        self.source_folder = self.scientific_metadata['app']['settings']['save_dir']

        H5Ingestor.get_dataset_metadata(self)

        # overwrite unique ID if one is in the file
        if 'unique_id' in self.h5file.attrs.keys():
            self.unique_id = self.h5file.attrs['unique_id']

        # overwrite creation time and data format
        self.timestamp = datetime.fromtimestamp(self.h5file.attrs['time_id']).isoformat()
        self.data_format = "ScopeFoundryH5"

        # parse session_name and tags
        default_tags_value = "list,tags,separated,by,commas (optional)"
        default_session_value = "(optional)"

        try: 
            scope_foundry_tags = self.scientific_metadata['hardware']['mf-crucible']['settings']['tags'].strip()
            scope_foundry_session = self.scientific_metadata['hardware']['mf-crucible']['settings']['session_name'].strip()

        except Exception:
            logger.warning("no mf-crucible settings found for tags or session_name")
            scope_foundry_tags = default_tags_value
            scope_foundry_session = default_session_value

        if scope_foundry_tags != default_tags_value:
            self.keywords += [x.strip() for x in scope_foundry_tags.split(",")]

        if scope_foundry_session != default_session_value:
            self.session_name = scope_foundry_session
            self.keywords += [self.session_name]


    def _find_measurement(self,k): 
        """regular expression tree walking function that
        Finds the first measurement in a ScopeFoundry HDF5
        k: key / path of hdf5 object (dataset or group)
        """
        r = re.compile("measurement/[^/]*$")
        if re.match(r, k): 
            return(k.split("/")[1])   

    def parse_orcid(self):
        if self.owner_orcid:
            return
        self.owner_orcid = check_orcid_entry(self.scientific_metadata['hardware']['mf_crucible']['settings']['orcid'])
        return


    def parse_project_id(self):
        if self.project_id:
            return
        else:
             self.project_id = self.scientific_metadata['hardware']['mf_crucible']['settings']['proposal'].split(" ")[0]
        return 




class ALDScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['ald_run_upd', 'ald_run', 'ald_run_measure']
    creation_location: ClassVar[str] = "67-4210"


class SimpleTiledImageScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['simple_tiled_image']
    creation_location: ClassVar[str] = "67-1207"


class CanonCaptureScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['canon_camera_capture']
    creation_location: ClassVar[str] = "67-1207"

    def get_thumbnails(self):
        image_file_name = f"{self.file_to_upload}.JPG"
        single_image = Image.open(image_file_name)
        self.add_thumbnail(single_image, "Canon Camera Capture")
        

class SingleSpecScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['picam_readout']
    creation_location: ClassVar[str] = "67-1217"

    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file[f"measurement/{self.measurement}"]
            spec = np.array(M['spectrum'])
            raman = np.array(M['raman_shifts'])
        plt.plot(raman, spec)
        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.clf()
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), "Picam Readout")
        


class HyperspecScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['m4_hyperspectral_2d_scan',
                              'andor_hyperspec_scan',
                              'hyperspectral_2d_scan',
                              'fiber_winspec_scan',
                              'hyperspec_picam_mcl',
                              'asi_OO_hyperspec_scan',
                              'oo_asi_hyperspec_scan',
                              'andor_asi_hyperspec_scan']

    creation_location: ClassVar[str] = "67-1217"
    
    def get_thumbnails(self):
        try:
            with h5py.File(self.file_to_upload, 'r') as h5file:
                M = h5file[f'measurement/{self.measurement}']
                spec_map = np.array(M['spec_map'])[0]
                wls = np.array(M['wls'])

            buf = BytesIO()
            plt.imsave(buf, spec_map.sum(axis=-1), origin='lower', format='png')
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), "Spectral Map")

            plt.plot(wls, spec_map.sum(axis=(0,1)))
            buf = BytesIO()
            plt.savefig(buf, format='png')
            plt.clf()
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), "Sum of Spectra")
        except Exception as err:
            logger.error(f"failed to generate thumbnail for {self.file_to_upload} due to error {err}")



class HyperspecSweepScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['hyperspec_picam_mcl_sweep']
    creation_location: ClassVar[str] = "67-1217"
    
    def get_thumbnails(self):
        pass


class ToupcamLiveScopeFoundryH5Ingestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['toupcam_live']
    creation_location: ClassVar[str] = "67-1217"
    
    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:

            if 'image' in h5file['measurement']['toupcam_live'].keys():
                imarray = np.array(h5file['measurement']['toupcam_live']['image'])
            else:
                logger.info(f"{h5file['measurement']['toupcam_live'].keys()=}")
                return
            
        h5image = Image.fromarray(imarray)
        self.add_thumbnail(h5image, "Toupcam Live Image")


class CLSyncRasterScanIngestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['sync_raster_scan']
    creation_location: ClassVar[str] = '67-1210'
    
    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file[f'measurement/{self.measurement}']
            if 'adc_map' in M.keys():
                adc_map = np.array(M['adc_map'])[0,0]
            else:
                adc_map = np.array([])
            
            if 'ctr_map' in M.keys():
                ctr_map = np.array(M['ctr_map'])[0,0]
            else:
                ctr_map = np.array([])
            logger.info(f"{adc_map.shape=}, {ctr_map.shape=}")

        # make a thumbnail for each channel in the ADC map
        for i in range(adc_map.shape[-1]):
            buf = BytesIO()
            plt.imsave(buf, adc_map[:,:,i], origin='lower', format='png')
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), f"ADC Channel {i}")

        # make a thumbnail for each channel in the Counter map
        for i in range(ctr_map.shape[-1]):
            buf = BytesIO()
            plt.imsave(buf, ctr_map[:,:,i], origin='lower', format='png')
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), f"Counter Channel {i}")


class CLHyperspecIngestor(ScopeFoundryH5Ingestor):

    supported_measurements: ClassVar[list[str]] = ['hyperspec_cl']
    creation_location: ClassVar[str] = '67-1210'

    def get_thumbnails(self):
        # Hyperspectral dataset include analog and counter
        # channels from sync_raster_scan, so we create thumbnails for those channels
        CLSyncRasterScanIngestor.get_thumbnails(self)

        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file[f'measurement/{self.measurement}']
            if not 'spec_map' in list(M.keys()):   
                return None
            spec_map = np.array(M['spec_map'])[0,0]
            wls = np.array(M['wls'])

        buf = BytesIO()
        plt.imsave(buf, spec_map.sum(axis=-1), origin='lower', format='png')
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), "Spectral Map")

        plt.plot(wls, spec_map.sum(axis=(0,1)))
        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.clf()
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), "Sum of Spectra")


class SpinBotIngestor(ScopeFoundryH5Ingestor):

    creation_location: ClassVar[str] = '67-4203'

    def get_dataset_metadata(self):
        ScopeFoundryH5Ingestor.get_dataset_metadata(self)
        
        default_tags_value = "list,tags,separated,by,commas (optional)"
        default_session_value = "(optional)"
        
        try: 
            scope_foundry_tags = self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['tags'].strip()
            scope_foundry_session = self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['session_name'].strip()

        except Exception:
            logger.warning("no mf-crucible settings found for tags or session_name")
            scope_foundry_tags = default_tags_value
            scope_foundry_session = default_session_value

        if scope_foundry_tags != default_tags_value:
            self.keywords += [x.strip() for x in scope_foundry_tags.split(",")]

        if scope_foundry_session != default_session_value:
            self.session_name = scope_foundry_session
            self.keywords += [self.session_name]

        self.keywords += [x for x in self.file_to_upload.split('/') if 'campaign' in x.lower()]
        self.keywords += [x for x in self.file_to_upload.split('/') if 'batch' in x.lower()]

    
    def parse_orcid(self):
        if self.owner_orcid:
            return
        self.owner_orcid = check_orcid_entry(self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['orcid'])
        return


    def parse_project_id(self):
        if self.project_id:
            return
        else:
            self.project_id = self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['proposal'].split(" ")[0]
        return 

    def parse_batch(self):
        full_batch_id = self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['batch_id']
        crucible_batch_id = full_batch_id.split('_')[2]
        batch_name = full_batch_id.split('_')[1]
        owner_orcid = self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['orcid']
        sample_info = {"unique_id": crucible_batch_id, "sample_name": batch_name, "owner_orcid": owner_orcid, "description": full_batch_id}
        self.batch = sample_info
        return(sample_info)

    def parse_samples(self):
        sample_label = self.scientific_metadata['app']['settings']['sample']
        logger.info(f"{sample_label=}")
        if sample_label is None:
            return
        elif len(sample_label) == 0:
            return
        else:
            logger.info(f"{sample_label}")
            
        owner_orcid = self.scientific_metadata['hardware']['mf_crucible_spinbot']['settings']['orcid']
        if len(sample_label) == 26 and sample_label.isalnum():
            sample_id = sample_label
        else:
            sample_id = None
            
        sample = {"unique_id": sample_id, 
                  "sample_name": sample_label, 
                  "owner_orcid": owner_orcid,
                  "parents": [{'unique_id': self.batch['unique_id']}]}
        
        # get the rest of the metadata
        self.samples.append(sample)
        return



class SpinbotSpecLineIngestor(SpinBotIngestor):

    supported_measurements: ClassVar[list[str]] = ['spec_line_scan']
 
    def get_thumbnails(self):
        try:
            with h5py.File(self.file_to_upload, 'r') as h5file:
                M = h5file[f"measurement/{self.measurement}"]
                spectra = np.array(M['spectra'])
                wls = np.array(M['wls'])
            for i in range(0, spectra.shape[0]):
                plt.plot(wls, spectra[i], label=f" spectra {i+1}")
            plt.legend()
            buf = BytesIO()
            plt.savefig(buf, format='png')
            plt.clf()
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), "SpinBot Spectra")
        except Exception as err:
            logger.error(f"failed to generate thumbnail for {self.file_to_upload} due to error {err}")



class SpinbotSpecRunIngestor(SpinBotIngestor):

    supported_measurements: ClassVar[list[str]] = ['spec_run']
    
    def make_spectra_plot(self, M, s, w):
        if len(M[s]) > 0:
            spectra = np.array(M[s])
            logger.info(f"spectra_shape={spectra.shape}")
            wls = np.array(M[w])
            for i in range(0, spectra.shape[0]):
                plt.plot(wls, spectra[i], label=f" spectra {i+1}")
            plt.legend()
            buf = BytesIO()
            plt.savefig(buf, format='png')
            plt.clf()
            buf.seek(0)
            return Image.open(buf)
        return None

    def get_thumbnails(self):
        try:
            with h5py.File(self.file_to_upload, 'r') as h5file:
                M = h5file[f"measurement/{self.measurement}"]
                dtypes = [x.split("_")[0] for x in list(M.keys()) if x.endswith("spectra")]
                for dtype in dtypes:
                    img = self.make_spectra_plot(M, f'{dtype}_spectra', f'{dtype}_wls')
                    if img is not None:
                        self.add_thumbnail(img, f"SpinBot {dtype.upper()} Spectra")

                if 'photo' in list(M.keys()):
                    imarray = np.array(M['photo'])
                    self.add_thumbnail(Image.fromarray(imarray), "SpinBot SpecRun Image")
        except Exception as err:
            logger.error(f"failed to generate thumbnail for {self.file_to_upload} due to error {err}")


class SpinbotCameraCaptureIngestor(SpinBotIngestor):

    supported_measurements: ClassVar[list[str]] = ['zwo_camera_capture']
    
    def get_thumbnails(self):
        for format in ['jpg', 'tif']:
            try:
                image_file_name = f"{os.path.basename(self.file_to_upload)}.{format}"
                single_image = Image.open(image_file_name)
                self.add_thumbnail(single_image, f"ZWO Capture ({format})")
            except Exception as tnfail:
                logger.error(f"failed to generate thumbnail for {self.file_to_upload} due to error {tnfail}")
        

class SpinbotPhotoRunIngestor(SpinBotIngestor):
    supported_measurements: ClassVar[list[str]] = ['photo_run']


class BioGlowIngestor(ScopeFoundryH5Ingestor):

    def is_file_supported(self):
        return self.file_to_upload.endswith('_bioglow_spec.h5')  


class QSpleemIngestor(ScopeFoundryH5Ingestor):
    """Base for QSpleem ingestors implementing the field convention:

        measurement -> human-readable name (e.g. 'LEEM-IV')
        data_type   -> 'ScopeFoundryH5.qspleem_<sf_group>.<subform>'

    The data_format prefix is hardcoded because ScopeFoundryH5Ingestor only sets
    self.data_format after parse_data_type() has already run.
    """
    _INSTRUMENT = 'qspleem'
    _GROUP = None            # ScopeFoundry measurement group, e.g. 'ARRES_EK'
    _LEED_THRESHOLD = 0.25   # median/p99 below this => diffraction (sparse frame)

    def _data_type(self, subform):
        return f"ScopeFoundryH5.{self._INSTRUMENT}_{self._GROUP.lower()}.{subform}"

    def _has_images(self):
        """True if this file carries a per-pixel diffraction stack."""
        try:
            return 'images' in self.h5file[f"measurement/{self._GROUP}"]
        except Exception:
            return False

    def _classify_plane(self, frame):
        """'diffraction' (LEED/SPLEED, sparse) or 'real_space' (LEEM/SPLEEM):
        a diffraction frame is mostly dark, so its median/p99 ratio is small."""
        frame = np.asarray(frame, dtype=np.float32)
        p99 = float(np.percentile(frame, 99)) or 1.0
        return 'diffraction' if float(np.median(frame)) / p99 < self._LEED_THRESHOLD else 'real_space'

    def _detect_imaging_mode(self, stack_path):
        """imaging_mode from a representative frame ~70% through an image stack;
        defaults to 'real_space' if the stack can't be read."""
        try:
            ds = self.h5file[stack_path]
            return self._classify_plane(ds[int(ds.shape[0] * 0.7)])
        except Exception:
            return 'real_space'

    def _add_diffraction_thumbnail(self, frame, caption):
        """Render one detector frame (p1-p99 grayscale) as a thumbnail."""
        frame = np.asarray(frame, dtype=np.float32)
        vmin, vmax = np.percentile(frame, 1), np.percentile(frame, 99)
        buf = BytesIO()
        plt.imsave(buf, frame, cmap='gray', format='png', origin='lower', vmin=vmin, vmax=vmax)
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), caption)


class QSpleemImageIngestor(QSpleemIngestor):
    supported_measurements: ClassVar[list[str]] = ['image_save']
    _GROUP = 'image_save'

    def is_file_supported(self):
        return(self.file_to_upload.endswith('_image_save.h5'))

    @functools.cached_property
    def imaging_mode(self):
        try:
            M = self.h5file['measurement/image_save']
            key = next(k for k in M.keys() if 'im_array' in k)
            return self._classify_plane(M[key][()])
        except Exception:
            return 'real_space'

    def parse_measurement(self):
        self.measurement = 'LEED Image' if self.imaging_mode == 'diffraction' else 'LEEM Image'

    def parse_data_type(self):
        self.data_type = self._data_type(self.imaging_mode)

    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file[f"measurement/image_save"]
            images = [k for k in list(M.keys()) if 'im_array' in k]
            buf = BytesIO()
            plt.imsave(buf, np.array(M[images[0]]), origin='lower', format='png')
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), "Qspleem Image 0")


class QSpleemSVRampIngestor(QSpleemIngestor):
    supported_measurements: ClassVar[list[str]] = ['sv_ramp']
    _GROUP = 'sv_ramp'

    def is_file_supported(self):
        return self.file_to_upload.endswith('_sv_ramp.h5')

    @functools.cached_property
    def imaging_mode(self):
        return self._detect_imaging_mode('measurement/sv_ramp/000_im_array')

    def parse_measurement(self):
        self.measurement = 'LEED-IV' if self.imaging_mode == 'diffraction' else 'LEEM-IV'

    def parse_data_type(self):
        self.data_type = self._data_type(self.imaging_mode)

    def get_thumbnails(self):
        try:
            with h5py.File(self.file_to_upload, 'r') as h5file:
                M = h5file['measurement/sv_ramp']
                sv    = np.array(M['0000_sv_array'])
                imavg = np.array(M['000_imavg_array'])
                wfs   = float(M['0000_wfs_array'][0]) if '0000_wfs_array' in M else None
                has_images = '000_im_array' in M

            # ── IV curve ──────────────────────────────────────────────────
            fig, ax = plt.subplots()
            ax.plot(sv, imavg, color='tab:blue')
            if wfs is not None:
                ax.axvline(wfs, color='gray', linestyle='--', linewidth=1, label=f'WF {wfs:.2f} V')
                ax.legend(loc='upper right')
            ax.set_xlabel('Start Voltage (V)')
            ax.set_ylabel('Image intensity (a.u.)')
            buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'SV Ramp IV Curve')

            # ── Image at max intensity after WF ───────────────────────────
            if has_images:
                if wfs is not None:
                    wf_idx = int(np.searchsorted(sv, wfs))
                    wf_idx = min(wf_idx, len(sv) - 2)
                else:
                    wf_idx = 0
                # Smooth and find bottom of the WF drop, then first peak after that
                smooth = np.convolve(imavg, np.ones(9)/9, mode="same")
                grad   = np.gradient(smooth, sv)
                sign_changes = np.where(np.diff(np.sign(grad[wf_idx:])) > 0)[0]
                min_idx = wf_idx + sign_changes[0] + 1 if len(sign_changes) > 0 else wf_idx + int(np.argmin(smooth[wf_idx:]))
                peak = min_idx + int(np.argmax(imavg[min_idx:]))
                with h5py.File(self.file_to_upload, 'r') as h5file:
                    frame = h5file['measurement/sv_ramp/000_im_array'][peak]
                fig, ax = plt.subplots()
                ax.imshow(frame, cmap='gray', origin='lower')
                ax.axis('off')
                wf_note = f' (post-WF {wfs:.2f} V)' if wfs is not None else ''
                ax.set_title(f'SV = {sv[peak]:.2f} V — max intensity{wf_note}', fontsize=8)
                buf = BytesIO(); plt.savefig(buf, format='png', dpi=150, bbox_inches='tight'); plt.clf(); buf.seek(0)
                self.add_thumbnail(Image.open(buf), f'Image at SV {sv[peak]:.2f} V (max post-WF)')

        except Exception as e:
            logger.warning(f'SVRamp thumbnail generation failed: {e}')


def _arres_robust_vlim(values, k):
    """Linear vmin/vmax that emphasizes the low-intensity (material) signal.

    The bright specular regions are a high-value minority where we are not
    probing the material, so a median + k*MAD ceiling keeps the color range on
    the low-intensity structure and lets the specular saturate. MAD adapts per
    dataset, avoiding fragile fixed percentiles. Zeros (unmeasured points) are
    excluded from the statistics.
    """
    m = values[np.isfinite(values) & (values != 0)]
    if m.size == 0:
        return None, None
    med = float(np.median(m))
    mad = float(np.median(np.abs(m - med)))
    vmin = float(np.percentile(m, 2))
    vmax = med + k * 1.4826 * mad if mad > 0 else float(np.percentile(m, 98))
    return vmin, vmax


class QSpleemARRESEKIngestor(QSpleemIngestor):
    _GROUP = 'ARRES_EK'
    _MAD_K = 3

    def is_file_supported(self):
        # base + image companions: _ARRES_EK.h5, _ARRES_EK_images.h5, _ARRES_EK_images_0000.h5
        return bool(re.search(r'_ARRES_EK(_images(_\d+)?)?\.h5$', self.file_to_upload))

    def parse_measurement(self):
        self.measurement = 'ARRES E(k)'

    def parse_data_type(self):
        self.data_type = self._data_type('diffraction' if self._has_images() else 'spectrum')

    def plotEK(self, M, spec, E, uv):
        uvmin = f"({str(round(uv[0][0],2))}, {str(round(uv[0][1], 2))})"
        uvmax = f"({str(round(uv[-1][0], 2))}, {str(round(uv[-1][1], 2))})"
        fig, ax = plt.subplots()
        vmin, vmax = _arres_robust_vlim(spec, self._MAD_K)
        ax.imshow(spec, origin="lower", vmin=vmin, vmax=vmax)
        fig.set_size_inches(10, 10)
        ax.set_aspect('auto')
        ax.set_xlim([0, len(uv)-1])
        ax.set_xticks([0, len(uv)-1], [uvmin, uvmax])
        ax.set_yticks(range(0, len(E), 5), [round(x,1) for i,x in enumerate(E) if i % 5 == 0])
        ax.set_ylabel("Energy (eV)")
        ax.set_xlabel("uv")
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=400)
        plt.clf()
        buf.seek(0)
        return Image.open(buf)

    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file["measurement/ARRES_EK"]
            if 'images' in M:
                # diffraction child: off-normal frame at 0.75 of max (u,v), at the highest energy
                spec = np.array(M['spectrum'])            # (spin, energy, k)
                E = np.array(M['eV'])
                uv = np.array(M['uv'])
                uv_max = uv[int(np.argmax(uv[:, 0] ** 2 + uv[:, 1] ** 2))]
                k_idx = int(np.argmin(np.sum((uv - 0.75 * uv_max) ** 2, axis=1)))
                e_idx = int(np.argmax(E))
                img = M['images']
                # Split image companions carry the FULL metadata (spectrum/eV/uv) but
                # only a slice of `images`; clamp the frame indices to what this file
                # actually holds so a representative frame is always in range — works
                # for any number of split parts.
                e_idx = min(e_idx, img.shape[1] - 1)
                k_idx = min(k_idx, img.shape[2] - 1)
                for spin in range(spec.shape[0]):
                    frame = img[spin, e_idx, k_idx]  # lazy (H, W) slice
                    self._add_diffraction_thumbnail(
                        frame, f"EK diffraction (spin {spin+1}, {E[e_idx]:.1f} eV, uv=({uv[k_idx][0]:.2f},{uv[k_idx][1]:.2f}))")
                return
            if not 'spectrum' in M.keys():
                return('no spectrum found')
            spec_series = np.array(M['spectrum'])
            E = np.array(M['eV'])
            uv = np.array(M['uv'])

        for i in range(0, spec_series.shape[0]):
            self.add_thumbnail(self.plotEK(M, spec_series[i, :, :], E, uv), f"QSpleem EK plot {i+1}")



class QSpleemARRESMMIngestor(QSpleemIngestor):
    _GROUP = 'ARRES_MM'
    _MAD_K = 3

    def is_file_supported(self):
        # base + image companions: _ARRES_MM.h5, _ARRES_MM_images.h5, _ARRES_MM_images_0000.h5
        return bool(re.search(r'_ARRES_MM(_images(_\d+)?)?\.h5$', self.file_to_upload))

    def parse_measurement(self):
        self.measurement = 'ARRES Constant Energy Surface'

    def parse_data_type(self):
        self.data_type = self._data_type('diffraction' if self._has_images() else 'momentum_map')

    def plotMM(self, spec, kx, ky, e):
        fig, ax = plt.subplots()
        vmin, vmax = _arres_robust_vlim(spec, self._MAD_K)
        disp = np.where(spec == 0, np.nan, spec)
        cmap = plt.get_cmap('viridis').copy()
        cmap.set_bad('lightgray')
        ax.imshow(disp, origin="lower", vmin=vmin, vmax=vmax, cmap=cmap)
        ax.set_ylabel("ky")
        ax.set_xlabel("kx")
        ax.set_title(f"Energy: {e} eV")
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=400)
        plt.clf()
        buf.seek(0)
        return Image.open(buf)

    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file["measurement/ARRES_MM"]
            if 'images' in M:
                # diffraction child: representative pattern at the 25th-percentile reflectivity point
                spec = np.array(M['spectrum'])            # (spin, kx, ky)
                img = M['images']
                # Split image companions carry the FULL metadata but only a slice of
                # `images`; clamp the frame indices to this file's actual shape so a
                # representative frame is always in range — works for any number of parts.
                for spin in range(spec.shape[0]):
                    s = spec[spin]
                    measured = s[np.isfinite(s) & (s != 0)]
                    if measured.size == 0:
                        continue
                    target = np.percentile(measured, 25)
                    s_masked = np.where(s == 0, np.nan, s)
                    idx = int(np.nanargmin(np.abs(s_masked - target)))
                    kxi, kyi = np.unravel_index(idx, s.shape)
                    kxi = min(int(kxi), img.shape[1] - 1)
                    kyi = min(int(kyi), img.shape[2] - 1)
                    frame = img[spin, kxi, kyi]   # lazy (H, W) slice
                    self._add_diffraction_thumbnail(frame, f"MM diffraction (spin {spin+1}, 25th pct reflectivity)")
                return
            spec_series = np.array(M['spectrum'])
            kx = np.array(M['kx'])
            ky = np.array(M['ky'])
            e = M['settings'].attrs['E']

        for i in range(0, spec_series.shape[0]):
            self.add_thumbnail(self.plotMM(spec_series[i, :, :], kx, ky, e), f"QSpleem Momentum Map {i+1}")


class NirvanaMultiPosLineScanIngestor(ScopeFoundryH5Ingestor):

    def is_file_supported(self):
        file_regex = f'.*pollux_oospec_multipos_line_scan.*\.h5'
        if re.match(file_regex, self.file_to_upload):
            return True
        else:
            return False
       # return(any([self.file_to_upload.endswith(f'_{x}.h5') for x in supported_measurements]))
    
    def get_dataset_metadata(self):
        self.instrument_name = 'Nirvana Spectrometer'
        self.source_folder = self.scientific_metadata['app']['settings']['save_dir']

        H5Ingestor.get_dataset_metadata(self)

        # overwrite unique ID if one is in the file
        if 'unique_id' in self.h5file.attrs.keys():
            self.unique_id = self.h5file.attrs['unique_id']

        # overwrite creation time and data format
        self.timestamp = datetime.fromtimestamp(self.h5file.attrs['time_id']).isoformat()
        self.data_format = "ScopeFoundryH5"

        # parse session_name and tags
        default_tags_value = "list,tags,separated,by,commas (optional)"
        default_session_value = "(optional)"

        try: 
            scope_foundry_tags = self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['tags'].strip()
            scope_foundry_session = self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['session_name'].strip()

        except Exception:
            logger.warning("no mf-crucible settings found for tags or session_name")
            scope_foundry_tags = default_tags_value
            scope_foundry_session = default_session_value

        if scope_foundry_tags != default_tags_value:
            self.keywords += [x.strip() for x in scope_foundry_tags.split(",")]

        if scope_foundry_session != default_session_value:
            self.session_name = scope_foundry_session
            self.keywords += [self.session_name]

    def parse_samples(self):
        pos_path = 'measurement/pollux_oospec_multipos_line_scan/positions'
        for pos in self.h5file[pos_path]:
            sample_id = self.h5file[pos_path][pos].attrs['sample_uuid']
            sample_name = self.h5file[pos_path][pos].attrs['sample_name']
            sample_description = pos
            if len(sample_id) > 0:
                sample = {"unique_id": sample_id, 
                          "sample_name": sample_name, 
                          "owner_orcid": self.owner_orcid,
                          "project_id": self.project_id}
                
                # get the rest of the metadata
                self.samples.append(sample)
        return

    def parse_orcid(self):
        if self.owner_orcid:
            return
        self.owner_orcid = check_orcid_entry(self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['orcid'])
        return

    def parse_project_id(self):
        if self.project_id:
            return
        else:
             self.project_id = self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['project'].split(" ")[0]
        return 


class QSpleemSVRampSpinIngestor(QSpleemIngestor):
    supported_measurements: ClassVar[list[str]] = ['sv_ramp_spin']
    _GROUP = 'sv_ramp_spin'

    def is_file_supported(self):
        return self.file_to_upload.endswith('_sv_ramp_spin.h5')

    @functools.cached_property
    def imaging_mode(self):
        return self._detect_imaging_mode('measurement/sv_ramp_spin/000_im_up_array')

    def parse_measurement(self):
        self.measurement = 'SPLEED-IV' if self.imaging_mode == 'diffraction' else 'SPLEEM-IV'

    def parse_data_type(self):
        self.data_type = self._data_type(self.imaging_mode)

    def get_thumbnails(self):
        try:
            with h5py.File(self.file_to_upload, 'r') as h5file:
                M = h5file['measurement/sv_ramp_spin']
                sv         = np.array(M['0000_sv_array'])
                imavg_up   = np.array(M['000_imavg_up_array'])
                imavg_down = np.array(M['000_imavg_down_array'])
                asym       = np.array(M['000_asym_array'])
                emga_up    = np.array(M['000_emga_up_array'])   if '000_emga_up_array'   in M else None
                emga_down  = np.array(M['000_emga_down_array']) if '000_emga_down_array' in M else None
                has_images = '000_im_up_array' in M

            # ── IV curves: spin up + spin down ────────────────────────────
            fig, ax = plt.subplots()
            ax.plot(sv, imavg_up,   color='tab:blue', label='Spin Up')
            ax.plot(sv, imavg_down, color='tab:red',  label='Spin Down')
            ax.set_xlabel('Start Voltage (V)')
            ax.set_ylabel('Mean Intensity (counts)')
            ax.legend(loc='upper right')
            buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'SV Ramp Spin IV Curves')

            # ── Asymmetry vs SV ───────────────────────────────────────────
            fig, ax = plt.subplots()
            ax.plot(sv, asym, color='tab:green')
            ax.set_xlabel('Start Voltage (V)')
            ax.set_ylabel('Asymmetry')
            buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'Spin Asymmetry vs SV')

            # ── Emission current (if present) ─────────────────────────────
            if emga_up is not None or emga_down is not None:
                fig, ax = plt.subplots()
                if emga_up   is not None: ax.plot(sv, emga_up,   color='tab:blue', label='Spin Up')
                if emga_down is not None: ax.plot(sv, emga_down, color='tab:red',  label='Spin Down')
                ax.set_xlabel('Start Voltage (V)')
                ax.set_ylabel('Emission Current (A)')
                ax.legend(loc='upper right')
                buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
                self.add_thumbnail(Image.open(buf), 'GaAs Emission Current vs SV')

            # ── 3-panel image at max |asymmetry| ─────────────────────────
            if has_images:
                window = 5
                asym_smooth = np.convolve(np.abs(asym), np.ones(window) / window, mode='same')
                peak = int(np.argmax(asym_smooth))
                with h5py.File(self.file_to_upload, 'r') as h5file:
                    M = h5file['measurement/sv_ramp_spin']
                    up   = M['000_im_up_array'][peak].astype(np.float32)
                    down = M['000_im_down_array'][peak].astype(np.float32)
                total = up + down
                asym_frame = np.where(total > 0, (up - down) / total, 0.0)

                def norm_gray(arr):
                    lo, hi = np.percentile(arr, 2), np.percentile(arr, 98)
                    return np.clip((arr - lo) / (hi - lo + 1e-9), 0, 1)

                fig, axes = plt.subplots(1, 3, figsize=(12, 4))
                axes[0].imshow(norm_gray(up),   cmap='gray',   origin='lower'); axes[0].set_title('Spin Up');   axes[0].axis('off')
                axes[1].imshow(norm_gray(down), cmap='gray',   origin='lower'); axes[1].set_title('Spin Down'); axes[1].axis('off')
                v = float(np.percentile(np.abs(asym_frame), 98)) or 1.0
                axes[2].imshow(asym_frame, cmap='RdBu_r', origin='lower', vmin=-v, vmax=v)
                axes[2].set_title('Asymmetry'); axes[2].axis('off')
                fig.suptitle(f'SV = {sv[peak]:.2f} V (max |asymmetry|)', fontsize=10)
                plt.tight_layout()
                buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
                self.add_thumbnail(Image.open(buf), f'Images at SV {sv[peak]:.2f} V (max |asym|)')

        except Exception as e:
            logger.warning(f'SVRampSpin thumbnail generation failed: {e}')


class QSpleemSPLEEMImageIngestor(QSpleemIngestor):
    supported_measurements: ClassVar[list[str]] = ['SPLEEM_image']
    _GROUP = 'SPLEEM_image'

    def is_file_supported(self):
        return self.file_to_upload.endswith('_SPLEEM_image.h5')

    def parse_measurement(self):
        self.measurement = 'SPLEEM Image'

    def parse_data_type(self):
        self.data_type = self._data_type('real_space')

    def get_thumbnails(self):
        try:
            with h5py.File(self.file_to_upload, 'r') as h5file:
                images = np.array(h5file['measurement/SPLEEM_image/images'], dtype=np.float32)
        except Exception as e:
            logger.warning(f'Could not read SPLEEM_image data: {e}')
            return

        avg_up   = images[:, 0, :, :].mean(axis=0)
        avg_down = images[:, 1, :, :].mean(axis=0)
        total    = avg_up + avg_down
        asym     = np.where(total > 0, (avg_up - avg_down) / total, 0.0)

        def to_img_gray(arr):
            lo, hi = np.percentile(arr, 2), np.percentile(arr, 98)
            arr = np.clip((arr - lo) / (hi - lo + 1e-9), 0, 1)
            buf = BytesIO()
            plt.imsave(buf, arr, cmap='gray', format='png', origin='lower')
            buf.seek(0)
            return Image.open(buf)

        def to_img_rdbu(arr):
            v = np.percentile(np.abs(arr), 98) or 1.0
            buf = BytesIO()
            plt.imsave(buf, arr, cmap='RdBu_r', vmin=-v, vmax=v, format='png', origin='lower')
            buf.seek(0)
            return Image.open(buf)

        self.add_thumbnail(to_img_gray(avg_up),   'SPLEEM Spin Up (averaged)')
        self.add_thumbnail(to_img_gray(avg_down), 'SPLEEM Spin Down (averaged)')
        self.add_thumbnail(to_img_rdbu(asym),     'SPLEEM Asymmetry (averaged)')


class QSpleemDepositionMonitorIngestor(QSpleemIngestor):
    supported_measurements: ClassVar[list[str]] = ['deposition_monitor']
    _GROUP = 'deposition_monitor'

    def is_file_supported(self):
        return self.file_to_upload.endswith('_deposition_monitor.h5')

    def parse_measurement(self):
        self.measurement = 'Deposition Monitor'

    def parse_data_type(self):
        self.data_type = self._data_type('time_series')

    def get_thumbnails(self):
        ROI_COLORS = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red']

        try:
            with h5py.File(self.file_to_upload, 'r') as f:
                M = f['measurement/deposition_monitor']
                images       = np.array(M['images'])
                roi_times    = np.array(M['roi_times'])
                roi_int      = np.array(M['roi_intensity'])
                ec           = np.array(M['emission_current'])
                pressure     = np.array(M['pressure_main_chamber'])
                temperature  = np.array(M['sample_temperature'])

            # Detect spin mode: images is (N, 2, H, W) vs (N, H, W)
            spin_mode = images.ndim == 4

            # Infer a scalar time axis for ec/pressure/temperature.
            # roi_times has shape (N,) or (N, 2); use column 0 for axis.
            t = roi_times[:, 0] if roi_times.ndim == 2 else roi_times

            # ── ROI intensity vs time ─────────────────────────────────────
            n_roi = roi_int.shape[-1]
            if n_roi > 0:
                fig, ax = plt.subplots()
                if spin_mode:
                    t_up   = roi_times[:, 0]
                    t_down = roi_times[:, 1]
                    for i in range(n_roi):
                        c = ROI_COLORS[i % len(ROI_COLORS)]
                        ax.plot(t_up,   roi_int[:, 0, i], color=c, linestyle='-',  label=f'ROI {i+1} Up')
                        ax.plot(t_down, roi_int[:, 1, i], color=c, linestyle='--', label=f'ROI {i+1} Down')
                else:
                    for i in range(n_roi):
                        ax.plot(t, roi_int[:, i], color=ROI_COLORS[i % len(ROI_COLORS)], label=f'ROI {i+1}')
                ax.set_xlabel('Time (s)')
                ax.set_ylabel('Intensity (counts)')
                ax.legend(fontsize=7)
                buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
                self.add_thumbnail(Image.open(buf), 'ROI Intensity vs Time')

            # ── ROI asymmetry vs time (spin mode only) ───────────────────
            if spin_mode and n_roi > 0:
                fig, ax = plt.subplots()
                t_up, t_down = roi_times[:, 0], roi_times[:, 1]
                for i in range(n_roi):
                    up, dn = roi_int[:, 0, i], roi_int[:, 1, i]
                    total = up + dn
                    asym = np.where(total > 0, (up - dn) / total, np.nan)
                    ax.plot(t_up, asym, color=ROI_COLORS[i % len(ROI_COLORS)], label=f'ROI {i+1}')
                ax.set_xlabel('Time (s)')
                ax.set_ylabel('Asymmetry')
                ax.legend(fontsize=7)
                buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
                self.add_thumbnail(Image.open(buf), 'ROI Asymmetry vs Time')

            # ── First frame with ROI boxes ────────────────────────────────
            roi_pos = np.array(M['roi_positions'])
            first = images[0, 0].astype(np.float32) if spin_mode else images[0].astype(np.float32)
            lo, hi = np.percentile(first, 2), np.percentile(first, 98)
            first = np.clip((first - lo) / (hi - lo + 1e-9), 0, 1)
            fig, ax = plt.subplots()
            ax.imshow(first, cmap='gray', origin='lower')
            for i in range(n_roi):
                x, y, w, h = roi_pos[-1, i]
                rect = plt.Rectangle((x, y), w, h, linewidth=1.5,
                                     edgecolor=ROI_COLORS[i % len(ROI_COLORS)], facecolor='none')
                ax.add_patch(rect)
                ax.text(x + w/2, y + h + 10, f'ROI {i+1}',
                        color=ROI_COLORS[i % len(ROI_COLORS)], ha='center', fontsize=7)
            ax.axis('off')
            ax.set_title('Final frame with ROI positions', fontsize=8, pad=4)
            buf = BytesIO(); plt.savefig(buf, format='png', dpi=150, bbox_inches='tight'); plt.clf(); buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'First Frame with ROIs')

            # ── Scalar time series — combined ─────────────────────────────
            fig, axes = plt.subplots(1, 3, figsize=(12, 3))
            for ax, arr, ylabel in zip(axes,
                [ec, pressure, temperature],
                ['Emission Current (A)', 'Pressure (mbar)', 'Temperature (°C)']):
                ax.plot(t, arr, color='tab:blue', linewidth=1)
                ax.set_xlabel('Time (s)'); ax.set_ylabel(ylabel)
            plt.tight_layout()
            buf = BytesIO(); plt.savefig(buf, format='png', dpi=150); plt.clf(); buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'Instrument Parameters vs Time')

            # ── Final frame asymmetry (spin mode only) ────────────────────
            if spin_mode:
                up_last   = images[-1, 0].astype(np.float32)
                down_last = images[-1, 1].astype(np.float32)
                total = up_last + down_last
                asym_frame = np.where(total > 0, (up_last - down_last) / total, 0.0)
                v = float(np.percentile(np.abs(asym_frame), 98)) or 1.0
                buf = BytesIO()
                plt.imsave(buf, asym_frame, cmap='RdBu_r', vmin=-v, vmax=v, format='png', origin='lower')
                buf.seek(0)
                self.add_thumbnail(Image.open(buf), 'Final Frame Asymmetry')

        except Exception as e:
            logger.warning(f'DepositionMonitor thumbnail generation failed: {e}')

        # overwrite unique ID if one is in the file
        if 'unique_id' in self.h5file.attrs.keys():
            self.unique_id = self.h5file.attrs['unique_id']

        # overwrite creation time and data format
        self.timestamp = datetime.fromtimestamp(self.h5file.attrs['time_id']).isoformat()
        self.data_format = "ScopeFoundryH5"

        # parse session_name and tags
        default_tags_value = "list,tags,separated,by,commas (optional)"
        default_session_value = "(optional)"

        try: 
            scope_foundry_tags = self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['tags'].strip()
            scope_foundry_session = self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['session_name'].strip()

        except Exception:
            logger.warning("no mf-crucible settings found for tags or session_name")
            scope_foundry_tags = default_tags_value
            scope_foundry_session = default_session_value

        if scope_foundry_tags != default_tags_value:
            self.keywords += [x.strip() for x in scope_foundry_tags.split(",")]

        if scope_foundry_session != default_session_value:
            self.session_name = scope_foundry_session
            self.keywords += [self.session_name]

    # def parse_orcid(self):
    #     if self.owner_orcid:
    #         return
    #     self.owner_orcid = check_orcid_entry(self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['orcid'])
    #     return


    # def parse_project_id(self):
    #     if self.project_id:
    #         return
    #     else:
    #          self.project_id = self.scientific_metadata['hardware']['mf_crucible_nirvana']['settings']['project'].split(" ")[0]
    #     return 












        









