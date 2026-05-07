import os
import re
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
    
    def get_dataset_metadata(self):
        self.instrument_name = self.scientific_metadata['app']['name']
        self.source_folder = self.scientific_metadata['app']['settings']['save_dir']
        self.measurement= self.h5file.visit(self._find_measurement)

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


class QSpleemImageIngestor(ScopeFoundryH5Ingestor):
    supported_measurements: ClassVar[list[str]] = ['image_save']

    def is_file_supported(self):
        return(self.file_to_upload.endswith('_image_save.h5'))

    
    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file[f"measurement/image_save"]
            images = [k for k in list(M.keys()) if 'im_array' in k]
            buf = BytesIO()
            plt.imsave(buf, np.array(M[images[0]]), origin='lower', format='png')
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), "Qspleem Image 0")


class QSpleemSVRampIngestor(ScopeFoundryH5Ingestor):
    supported_measurements: ClassVar[list[str]] = ['sv_ramp']

    def is_file_supported(self):
        return(self.file_to_upload.endswith('_sv_ramp.h5'))
    
    def plot_image_at_diffpeak(self, M, image_array_key):
        descriptions = {"000_im_array":"",
                        "000_im_up_array": " (Spin Up)",
                        "000_im_down_array": " (Spin Down)"}
        im_descrip = descriptions[image_array_key]
        sv_ramp = np.array(M['0000_sv_array'])
        im = np.array(M[image_array_key])
        imavg = np.array(M['000_imavg_array'])
        diffpeak = np.argmax(imavg)
        plt.imshow(im[diffpeak,:,:])
        plt.tick_params(which='both', size=0, labelsize=0)
        plt.title(f"Diffraction at SV {sv_ramp[diffpeak]}{im_descrip}")
        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.clf()
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), f"QSpleem SV Ramp Diffraction{im_descrip}")

    def plot_basic_image(self, M, image_array_key):
        im = np.array(M[image_array_key])
        if len(im.shape) != 2:
            return f'unexpected image shape {im.shape} - expecting 2d array'
        plt.imshow(im)
        plt.colorbar()
        plt.tick_params(which='both', size=0, labelsize=0)
        plt.title(image_array_key)
        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.clf()
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), image_array_key)

    def plot_average(self, M):
        sv_ramp = np.array(M['0000_sv_array'])
        svarrays = [x for x in list(M.keys()) if 'imavg_array' in x]
        for x in svarrays:
            arr = np.array(M[x])
            if arr.shape[0] == sv_ramp.shape[0]:
                plt.plot(sv_ramp, arr)
        plt.xlabel("Energy (eV)")
        plt.ylabel("Average Reflectivity")
        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.clf()
        buf.seek(0)
        self.add_thumbnail(Image.open(buf), "QSpleem SV Ramp Average")

    def get_thumbnails(self):
        with h5py.File(self.file_to_upload, 'r') as h5file:
            M = h5file[f"measurement/{self.measurement}"]

            if '0000_sv_array' in M.keys():
                self.plot_average(M)

            if '000_im_array' in M.keys():
                self.plot_image_at_diffpeak(M, '000_im_array')

            other_im_keys = ['000_im_up_array', '000_im_down_array']
            for k in [x for x in other_im_keys if x in list(M.keys())]:
                self.plot_basic_image(M, k)


class QSpleemARRESEKIngestor(ScopeFoundryH5Ingestor):

    def is_file_supported(self):
        supported_measurements = ['ARRES_EK']
        return(any([self.file_to_upload.endswith(f'_{x}.h5') for x in supported_measurements]))


    def plotEK(self, M, spec, E, uv):
        uvmin = f"({str(round(uv[0][0],2))}, {str(round(uv[0][1], 2))})"
        uvmax = f"({str(round(uv[-1][0], 2))}, {str(round(uv[-1][1], 2))})"
        fig, ax = plt.subplots()
        ax.imshow(spec, origin="lower")
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
            M = h5file[f"measurement/{self.measurement}"]
            if not 'spectrum' in M.keys():
                return('no spectrum found')
            spec_series = np.array(M['spectrum'])
            E = np.array(M['eV'])
            uv = np.array(M['uv'])

        for i in range(0, spec_series.shape[0]):
            self.add_thumbnail(self.plotEK(M, spec_series[i, :, :], E, uv), f"QSpleem EK plot {i+1}")



class QSpleemARRESMMIngestor(ScopeFoundryH5Ingestor):

    def is_file_supported(self):
        supported_measurements = ['ARRES_MM']
        return(any([self.file_to_upload.endswith(f'_{x}.h5') for x in supported_measurements]))

    
    def plotMM(self, spec, kx, ky, e):
        fig, ax = plt.subplots()
        ax.imshow(spec, origin="lower")
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
            M = h5file[f"measurement/{self.measurement}"]
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
        self.measurement= self.h5file.visit(self._find_measurement)

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












        









