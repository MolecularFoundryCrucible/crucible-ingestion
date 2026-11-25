import os
import shutil
from datetime import datetime as dt
from datetime import timezone
import logging
import zipfile
import pandas as pd
from pathlib import Path

from .crucible_ingestor import CrucibleDatasetIngestor, TMP_DIR
from ..client import get_client
logger = logging.getLogger(__name__)


def _find_readout_files(directory):
    return [
        f for f in os.listdir(directory)
        if not f.startswith(".")
        and f.startswith("sample_holder_position_readout_")
        and f.endswith(".txt")
    ]


# from Kas / Ed code base
def build_sample_table(directory, samples_by_name):
    logger.info(os.listdir(directory))
    logger.info(directory)

    # check if extracted files got double nested
    subfolder_name = Path(directory).name
    if os.path.isdir(os.path.join(directory, subfolder_name)):
        directory = os.path.join(directory, subfolder_name)
        logger.info(f'Found subfolder in extract_path directory: updating directory to {directory=}')

    matching_files = _find_readout_files(directory)
    if not matching_files:
        subdirs = [
            d for d in os.listdir(directory)
            if not d.startswith(".") and d != "__MACOSX" and os.path.isdir(os.path.join(directory, d))
        ]
        if len(subdirs) == 1:
            directory = os.path.join(directory, subdirs[0])
            logger.info(f"Descending into single subdirectory for readout files: {directory=}")
            matching_files = _find_readout_files(directory)

    if not matching_files:
        raise FileNotFoundError("No 'sample_holder_position_readout_*.txt' files found.")

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
        conditions = [self.match_name.lower().endswith('.zip'), '10k_RGA' in self.match_name]
        return all(conditions)

    def get_scientific_metadata(self):
        """
        Base function that gets called
        during setup_data() - should
        populate the metadata_dictionary
        of the object.
        """
        os.makedirs(TMP_DIR, exist_ok=True)
        extract_stem = Path(self.match_name).stem or os.path.basename(self.file_to_upload)
        extract_path = os.path.join(TMP_DIR, extract_stem)
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        with zipfile.ZipFile(self.file_to_upload) as zf:
            zf.extractall(extract_path)
        logger.info(f'{extract_path=}')
        logger.info(f'{os.listdir(TMP_DIR)=}')
        project_id = self.project_id or '10k_perovskites'
        samples_in_project = get_client().samples.list(project_id=project_id, sample_type='thin film', limit=None)
        samples_by_name = {sample["sample_name"]: sample for sample in samples_in_project}

        df = build_sample_table(extract_path, samples_by_name)
        # Multiple readout files can cover the same physical sample spot. Collapse
        # rows that agree on every column; a spot with differing rows is a conflict.
        df = df.drop_duplicates()
        conflicts = df[df.duplicated(subset="sample spot", keep=False)]["sample spot"].unique().tolist()
        if conflicts:
            raise ValueError(f"Conflicting readouts for sample spot(s): {conflicts}")
        self.scientific_metadata["samples"] = df.set_index("sample spot").to_dict(orient="index")
        logger.info(f'{self.scientific_metadata["samples"]=}')

    def parse_file_timestamp(self):
        if self.timestamp:
            return
        else:
            file_ctime = os.path.getmtime(self.file_to_upload)
            self.timestamp = dt.fromtimestamp(file_ctime, tz=timezone.utc).isoformat()
            logger.info(f"{self.timestamp=}")
            return

