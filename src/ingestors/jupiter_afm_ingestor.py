import os
from typing import ClassVar
import numpy as np
import requests
import logging
import matplotlib.pyplot as plt
from PIL import Image
import igor2 as igor

import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from ..utils import get_secret
from ..google_calendar import (find_calendar_event,
                               parse_calendar_event_for_ownership)
from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

from crucible import CrucibleClient
crucible_api_url = os.environ.get('CRUCIBLE_API_URL')
crucible_apikey = get_secret("ADMIN_APIKEY", "crucible_admin_apikey/versions/4")


def decode_recurse(x):
    if isinstance(x, dict):
        for k in list(x.keys()):
            x[k] = decode_recurse(x[k])
    elif isinstance(x, bytes):
        try:
            x = x.decode('latin-1').replace('\u0000', '')
        except Exception:
            x = x
    elif isinstance(x, list):
        x = [decode_recurse(i) for i in x]
    elif isinstance(x, np.ndarray):
        x = np.array([decode_recurse(i) for i in x])
    else:
        x = x     
    return(x)


def parse_note(raw_note):
    """Your existing note parser, extracted as a function."""
    newnote = {}
    for y in [x.split(":") for x in raw_note.split("\r")]:
        if len(y) == 2:
            newnote[y[0].strip()] = y[1].strip()
        elif len(y) > 2:
            newnote[y[0].strip()] = ":".join(y[1:]).strip()
        elif y[0] != "":
            newnote[y[0].strip()] = None
        else:
            continue
    for x in ['SaveImage', 'SaveForce', 'LastSaveImage', 'LastSaveForce']:
        if x in newnote and newnote[x] is not None:
            newnote[x] = newnote[x].replace(":", "/")
    return newnote


def safe_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def extract_channel_names(labels):
    """Extract non-empty channel names from labels[2]."""
    try:
        return [l for l in labels[2] if l and l.strip()]
    except (IndexError, TypeError):
        return []


def compute_channel_stats(wData, channel_names):
    """Compute per-channel quality stats."""
    stats = {}
    # wData shape: (nx, ny, n_channels) but channel index may be offset by 1
    # labels[2] has a leading empty string, so channel 'HeightRetrace' is index 1
    for i, name in enumerate(channel_names):
        ch_idx = i + 1  # offset for leading empty label
        try:
            channel = wData[:, :, ch_idx].astype(np.float64)
            finite_vals = channel[np.isfinite(channel)]
            stats[name] = {
                "min": float(np.min(finite_vals)) if len(finite_vals) else None,
                "max": float(np.max(finite_vals)) if len(finite_vals) else None,
                "std": float(np.std(finite_vals)) if len(finite_vals) else None,
                "range": float(np.ptp(finite_vals)) if len(finite_vals) else None,
                "has_nan_or_inf": bool(not np.all(np.isfinite(channel))),
                "fraction_finite": float(len(finite_vals) / channel.size),
            }
        except (IndexError, ValueError) as e:
            stats[name] = {"error": str(e)}
    return stats

    
class AFMIngestor(CrucibleDatasetIngestor):
    supported_filetypes: ClassVar[list[str]] = ['ibw']

    def is_file_supported(self):
        return np.any([self.file_to_upload.endswith(ftype) for ftype in self.supported_filetypes])

        
    def get_scientific_metadata(self):
        # --- Load and decode ---
        im = igor.binarywave.load(self.file_to_upload)
        im = decode_recurse(im)
        
        note = parse_note(im['wave']['note'])
        wh = im['wave']['wave_header']
        wData = im['wave']['wData']
        labels = im['wave']['labels']
        channel_names = extract_channel_names(labels)
    
        # --- Spatial ---
        nDim = wh['nDim']
        nx, ny, n_channels = int(nDim[0]), int(nDim[1]), int(nDim[2])
        sfA = wh['sfA']
        pixel_size_m = float(sfA[0])
        scan_size_x_m = pixel_size_m * nx
        scan_size_y_m = float(sfA[1]) * ny
    
        # --- Channel stats ---
        channel_stats = compute_channel_stats(wData, channel_names)
    
        # Convenience: pull Height stats to top level if available
        height_key = next((k for k in channel_names if 'Height' in k), None)
        height_stats = channel_stats.get(height_key, {}) if height_key else {}
    
        # --- Build metadata dict ---
        self.scientific_metadata = {
            # ---- Identity ----
            "ibw_version": int(im['version']),
            "file_last_modified": wh.get('modDate'), # gets popped into dataset metadata as timestamp
            
            # ---- Spatial ----
            "image_size_px": [nx, ny],
            "is_square": nx == ny,
            "pixel_size_m": pixel_size_m,
            "scan_size_x_m": scan_size_x_m,
            "scan_size_y_m": scan_size_y_m,
            "x_offset_m": float(sfA[1]) if len(sfA) > 1 else None,  # sfB would be offset
    
            # ---- Channels ----
            "n_channels": n_channels,
            "channel_names": channel_names,
            "has_height": any('Height' in c for c in channel_names),
            "has_phase": any('Phase' in c for c in channel_names),
            "has_amplitude": any('Amplitude' in c for c in channel_names),
            "has_zsensor": any('ZSensor' in c for c in channel_names),
            "data_type": str(wData.dtype),
    
            # ---- Data quality (top-level height channel) ----
            "height_z_range_nm": height_stats.get('range', None) and height_stats['range'] * 1e9,
            "height_z_std_nm": height_stats.get('std', None) and height_stats['std'] * 1e9,
            "height_has_nan_or_inf": height_stats.get('has_nan_or_inf', None),
            "height_fraction_finite": height_stats.get('fraction_finite', None),
    
            # ---- Per-channel stats (nested) ----
            "channel_stats": channel_stats,
    
            # ---- Acquisition (from note) ----
            "imaging_mode": note.get('ImagingMode'),
            "scan_rate_hz": safe_float(note.get('ScanRate')),
            "scan_angle_deg": safe_float(note.get('ScanAngle')),
            "setpoint": safe_float(note.get('Setpoint')),
            "setpoint_units": note.get('SetpointUnits'),
            "drive_amplitude_v": safe_float(note.get('DriveAmplitude')),
            "drive_frequency_hz": safe_float(note.get('DriveFrequency')),
            "spring_constant_n_m": safe_float(note.get('SpringConstant')),
            "deflection_invols": safe_float(note.get('InvOLS')),
            "z_range_m": safe_float(note.get('FastMapZRange') or note.get('ZRange')),
    
            # ---- Sample/experiment context (from note) ----
            "x_offset_um": safe_float(note.get('XOffset')) and safe_float(note.get('XOffset')) * 1e6,
            "y_offset_um": safe_float(note.get('YOffset')) and safe_float(note.get('YOffset')) * 1e6,
            "scan_angle_deg": safe_float(note.get('ScanAngle')),
            "tip_serial": note.get('MicroscopeID'),
            "temperature": safe_float(note.get('Temperature')),
            "humidity": safe_float(note.get('Humidity')),
        }

    
    def parse_dataset_name(self):
        informative_path = self.file_to_upload.split("Asylum Research Data")[-1]
        self.dataset_name = informative_path.strip("/").replace("/", "-") 

    
    def parse_file_timestamp(self):
        lastMod = self.scientific_metadata.pop('file_last_modified')
        try:
            igor_epoch = datetime(1904, 1, 1)
            last_modified_isoformat = (igor_epoch + timedelta(seconds=int(lastMod))).isoformat()
            self.timestamp = last_modified_isoformat
        except Exception as err:
            print(f'{err=}')
            self.timestamp = None

    
    def parse_instrument(self):
        self.instrument_name = 'jupiterafm'
        self.acl.append(self.instrument_name)

    
    def parse_measurement(self):
        """
        Returns measurement type string:
        - Single channel: "ImagingMode::ChannelName"
        - Multi channel:  "MultiChannelAFM - Mode1::Ch1,Ch2,..."
        
        Assumes all channels share the same imaging mode (typical for Jupiter AFM).
        """
        imaging_mode = self.scientific_metadata['imaging_mode']
        print(f'{imaging_mode=}')
        channel_names = self.scientific_metadata['channel_names']
        print(f'{channel_names=}')
        
        if not channel_names:
            self.measurement = imaging_mode
        
        if len(channel_names) == 1:
            self.measurement = f"{imaging_mode}::{channel_names[0]}"
        else:
            channel_str = ",".join(channel_names)
            self.measurement =  f"MultiChannelAFM - {imaging_mode}::{channel_str}"

            
    def parse_keywords(self):
        parent_folder = os.path.dirname(self.file_to_upload).split('/')[-1]
        self.keywords = [self.instrument_name,
                         self.measurement,
                         self.data_type,
                         parent_folder]
    


    def parse_orcid(self):
        client = CrucibleClient(crucible_api_url, crucible_apikey)
        
        if self.owner_orcid:
            return
        
        cal_id = 'c_550eaa9a91952a820fb6d76a3306f5583abcffc7cf42e72573fd2a0cae1b1c8f@group.calendar.google.com'
        sa_file = f"{os.getenv('HOME')}/.config/mf-crucible-9009d3780383.json"
        cal_event = find_calendar_event(self.timestamp, cal_id, service_account_file = sa_file)
        
        if cal_event:
            self.email, self.project_id = parse_calendar_event_for_ownership(cal_event)
            try:
                user_info = client.users.get(self.email)
                self.owner_orcid = user_info['unique_id']
            except:
                logger.info(f'user with email {self.email} not found')
        else:
            return


    def parse_project_id(self):
                
        cal_id = 'c_550eaa9a91952a820fb6d76a3306f5583abcffc7cf42e72573fd2a0cae1b1c8f@group.calendar.google.com'
        sa_file = f"{os.getenv('HOME')}/.config/mf-crucible-9009d3780383.json"
        if not self.project_id:
            cal_event = find_calendar_event(self.timestamp, cal_id, service_account_file = sa_file)
            if cal_event:
                self.email, self.project_id = parse_calendar_event_for_ownership(cal_event)
        
        if not self.project_id:
            return
        
        if "Internal Research" in self.project_id and self.email is not None:
            self.project_id = f"MFUSER_{self.email.split('@')[0]}"

    
    def make_retrace_plot(self, array, pname):
        spec_map_filename = f"{os.path.basename(self.file_to_upload)}_{pname}.png"

        fig, ax = plt.subplots()
        ax.imshow(array, cmap='Greys')
        ax.axis('off')

        fig.savefig(spec_map_filename, dpi=200, bbox_inches='tight', pad_inches=0)
        plt.close(fig)
        return(Image.open(spec_map_filename))

    
    def get_thumbnails(self):
        im = igor.binarywave.load(self.file_to_upload)
        
        w = np.array(im['wave']['wData'])
        labels = [x.decode('latin-1') for x in im['wave']['labels'][2] if x.decode('latin-1') != ""]
        if len(w.shape) == 2:
            traceim = self.make_retrace_plot(w, "AFM Image")
            self.add_thumbnail(traceim, "AFM Image")
        elif len(w.shape) == 3:
            traceim = self.make_retrace_plot(w[:,:,1], labels[1])
            self.add_thumbnail(traceim, labels[1])
        else:
            logger.error(f"Failed to add thumbnail - wData had dim: {w.shape}")
            return(w.shape)
        




    

























