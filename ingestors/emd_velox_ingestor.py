import os
import re
import io

from pathlib import Path
from PIL import Image
import numpy as np
import ncempy.io as nio
import matplotlib.pyplot as plt
import logging
import json

from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def parse_dataset_as_dict(dataset): 
    """
    Turns a h5py Dataset of type "O" into a python dict.
    Helper function for processing certain data from the h5py file. 
    Modeled after rsciio.

    Args:
    - dataset: an h5py Dataset object of type "O"

    Returns: 
    - dataset decoded as a dict 
    """
    bytes = dataset[()].tolist()[0]
    decoded = bytes.decode("utf-8", "ignore").rstrip("\x00")
    return json.loads(decoded)

get_groupType_from_md = lambda md: md['General']['groupType'] if 'groupType' in md['General'] else "" # options: STEM, TEM, EDS_Processed, EDS
get_title_from_md = lambda md: md['General']['title'] if 'title' in  md['General'] else "" # options: EDS_SI, EDS_Spectrum, HAADF, element name

class fileEMDVeloxWithSpectra(nio.emdVelox.fileEMDVelox):
    PROCESSED_IMAGE_GROUP_NAME = 'EDS_Processed'
    SPECTRUM_IMAGE_GROUP_NAME = 'EDS_SI'
    SPECTRUM_GROUP_NAME = 'EDS_Spectrum'

    def __init__(self, filename): 
        super().__init__(filename)
        self._parse_image_titles()

    def _find_groups(self):
        """ 
        Find all data groups: spectrum data, image data, and spectrum image. (in that order)
        Spectrum are omitted unless there are no other data types.
        """
        self.list_data = []
        groups_to_extract = ['Data/Image', 'Data/SpectrumImage']
        for group in groups_to_extract: 
            if group in self._file_hdl:
                self.list_data += list(self._file_hdl[group].values())
        
        # only add Spectrum data (to the front of list_data) if there are no Images or Spectrum Images 
        if len(self.list_data) == 0 and 'Data/Spectrum' in self._file_hdl: 
            self.list_data = list(self._file_hdl['Data/Spectrum'].values()) + self.list_data

    def _parse_image_titles(self):
        """
        Create a mapping between each image_uuid: 
            - 'groupType' (EDS, STEM, TEM)
            - 'title' (element name or HAADF)
        Stores the mapping for in self.img_titles
        """
        self.img_titles = {}
        
        if "SharedProperties/DisplayGroupItem" not in self._file_hdl: 
            return 
        
        display_groups = self._file_hdl["SharedProperties/DisplayGroupItem"]
        for group in display_groups.values():
            display_group_dict = parse_dataset_as_dict(group)
            # /Displays/ImageDisplay/___
            image_display_path = display_group_dict['display']
            image_display_dict = parse_dataset_as_dict(self._file_hdl.get(image_display_path))
            
            # /SharedProperties/ImageSeriesDataReference/___
            data_ref_path = image_display_dict["data"]
            data_ref_dict = parse_dataset_as_dict(self._file_hdl.get(data_ref_path))

            data_path = data_ref_dict["dataPath"]
            # data_path = data_path.split("/")[-1]
            groupType = display_group_dict['groupType'].upper()
            self.img_titles[data_path] = {'groupType': self.PROCESSED_IMAGE_GROUP_NAME if groupType == "EDS" else groupType, 
                                          'title': display_group_dict['name']} 

    def getMetadata(self, group):
        """ Reads important metadata from Velox EMD files.

        Parameters
        ----------
        group : h5py.Group or int
            The h5py group to load the metadata from which is easily retrived from the list_data attribute.
            If input is an int then the group corresponding to list_data attribute is used. The string 
            metadata is loaded and parsed by the json module into a dictionary.
        """
        # ensure group is an actual group object, not an int 
        try:
            if type(group) is int:
                group = self.list_data[group]
        except IndexError:
            raise IndexError('EMDVelox group #{} does not exist.'.format(group))
        
        meta_data = super().getMetadata(group)
       
        # add general metadata
        general_md = {}
        if group.name in self.img_titles: 
            general_md = self.img_titles[group.name]
        elif group.parent.name == '/Data/Spectrum':
            general_md = {'groupType': self.SPECTRUM_GROUP_NAME, 'title': self.SPECTRUM_GROUP_NAME} 
        elif group.parent.name == '/Data/SpectrumImage':
            general_md = {'groupType': self.SPECTRUM_IMAGE_GROUP_NAME, 'title': self.SPECTRUM_IMAGE_GROUP_NAME} 

        meta_data.update({'General': general_md})

        return meta_data

    def getThumbnailImageDataset(self):
        """
        Return an image dataset for thumbnail-generation. 
        If a non-processed-image exists, return an arbitrary one. 
        Otherwise, return an arbitrary processed-image. 
        """
        if '/Data/Image' not in self._file_hdl: 
            return None

        # choose the image from which to create a thumbnail
        image_dataset = None
        # iterate through image datasets to find a non-processed image 
        all_image_groups = list(self._file_hdl['/Data/Image'].values())
        for group in all_image_groups[::-1]:  # start from the back, assuming that processed images are at the front
            md = self._ncempy_datafile.getMetadata(group)
            if get_groupType_from_md(md) != self._ncempy_datafile.PROCESSED_IMAGE_GROUP_NAME: 
                image_dataset = group 
                break 
        # if all images are processed images, use an arbitrary processed image
        if image_dataset == None: 
            image_dataset = all_image_groups[0]
        return image_dataset['Data'][:,:,0]
        
class VeloxEmdIngestor(CrucibleDatasetIngestor):
    '''subclass for ingesting Velox EMD files'''

    def is_file_supported(self):
        if not self.file_to_upload.endswith('.emd'):
            return False
        
        with fileEMDVeloxWithSpectra(self.file_to_upload, readonly=True) as emd1:
            return len(emd1.list_data) > 0
            # this only ensures that there's at least a Image, Spectrum Image, or Spectrum group? 

        #TODO: modify

    
    def get_scientific_metadata(self):
        with fileEMDVeloxWithSpectra(self.file_to_upload, readonly=True) as emd1:
            if len(emd1.list_data) == 1: 
                self.scientific_metadata = emd1.getMetadata(0)
            else: # TODO: use a list of child metadata dictionaries, labeled by their measurement
                self.scientific_metadata = {}
        logger.info(f'Got metadata from Velox EMD: {self.scientific_metadata=}')
         
        # emd_handle = nio.emd.fileEMD(self.file_to_upload, readonly=True)
        # for device_index, device_name in enumerate(_dset_names(emd_handle)):
        #     logger.info(f'{device_index=}, {device_name=}')
        #     frame_stream_name = f'primary_{device_name}'
        #     stream_metadata = _metadata_from_dset(self.file_to_upload, dset_num=device_index)
        #     self.scientific_metadata[frame_stream_name] = stream_metadata
        # print(f'{self.scientific_metadata=}')
        # return

    def get_dataset_metadata(self):
         # Use parent class method to set data_format, size, and source_folder
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.dataset_name = Path(self.file_to_upload).stem # file name without extension
        # TODO: parse this
        self.measurement = ''

    def parse_dataset_name(self):
        if self.dataset_name:
            return
        else:
            # no_ext_fname = os.path.splitext(self.file_to_upload)[0]
            self.dataset_name = os.path.basename(self.file_to_upload) # keep the file extension
            logger.info(f"{self.dataset_name=}")
            return

    def generate_thumbnail(self):
        """Generate a thumbnail from an EMD image as a PNG.

        Returns
        -------
        : PIL.Image
            Thumbnail image as a PIL Image object.
        """
        target_size = (200, 200) # pixels
        dpi = 100
        fig_size = (target_size[0] / dpi, target_size[1] / dpi) # inches
       
        try:            
            with fileEMDVeloxWithSpectra(self.file_to_upload, readonly=True) as emd1:
                image_array = emd1.getThumbnailImageDataset()
            
            if image_array: 
                fg, ax = plt.subplots(1, 1, figsize=fig_size, dpi=dpi)
                ax.imshow(image_array, cmap = 'gray')
                ax.axis('off')

                # Convert to PIL Image and store in self.thumbnails
                buf = io.BytesIO()
                fg.savefig(buf, bbox_inches='tight', pad_inches=0.05, dpi=100)
                im = Image.open(buf)
                return im
            return None
        except Exception as e:
            print(f"Failed to generate thumbnail: {e}")

    def get_thumbnails(self):
        try:
            thumbnail = self.generate_thumbnail()
            if thumbnail:
                self.add_thumbnail(thumbnail, "EMD_Thumbnail")
        except Exception as e:
            print(f"Failed to extract thumbnail: {e}")