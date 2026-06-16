import os
from datetime import datetime as dt
from datetime import timezone
import logging
import zipfile
import pandas as pd
from .utils import get_secret
from crucible import CrucibleClient
from .crucible_ingestor import CrucibleDatasetIngestor
logger = logging.getLogger(__name__)

# Crucible Client
apikey = get_secret("ADMIN_APIKEY", "crucible_admin_apikey/versions/4")
crucible_api_url = os.environ.get('CRUCIBLE_API_URL')

client = CrucibleClient(api_url=crucible_api_url, api_key=apikey)


# from Kas / Ed code base
def build_sample_table(directory, samples_by_name):
    logger.info(os.listdir(directory))
    logger.info(directory)
    matching_files = [
        f for f in os.listdir(directory)
        if f.startswith("sample_holder_position_readout_") and f.endswith(".txt")
    ]
    if not matching_files:
        raise FileNotFoundError("No 'sample_holder_position_readout_YYYY-MM-DD.txt' files found.")

    matching_files.sort(reverse=True)
    logger.info(f"Using {len(matching_files)} position readout file(s): {matching_files}")

    df = pd.concat(
        [pd.read_csv(os.path.join(directory, f), sep="\t") for f in matching_files],
        ignore_index=True,
    )

    df["sample_id"] = df["sample_name"].map(
        lambda name: samples_by_name[name]["unique_id"] if name in samples_by_name else None
    )

    missing = df[df["sample_id"].isna()]["sample_name"].tolist()
    if missing:
        logger.warning(f"[WARN] {len(missing)} sample(s) not found in Crucible project: {missing}")

    return df

    
class RgaTeyBatchIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        conditions = [self.file_to_upload.endswith('zip'), '10k_RGA' in self.file_to_upload]
        return all(conditions)
        

    def get_scientific_metadata(self):
        """
        Base function that gets called
        during setup_data() - should
        populate the metadata_dictionary
        of the object.
        """
        with zipfile.ZipFile(self.file_to_upload) as zf:
            zf.extractall()
        
        samples_by_name = client.samples.list(project_id = '10k_perovksites', sample_type = 'thin film', limit = 10000)
        df = build_sample_table(self.file_to_upload.replace('.zip', ''), samples_by_name)
        self.scientific_metadata["samples"] = df.set_index("sample spot").to_dict(orient="index")
    

    def parse_file_timestamp(self):
        if self.timestamp:
            return
        else:
            file_ctime = os.path.getmtime(self.file_to_upload)
            self.timestamp = dt.fromtimestamp(file_ctime, tz=timezone.utc).isoformat()
            logger.info(f"{self.timestamp=}")
            return

