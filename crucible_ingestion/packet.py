import json
from dataclasses import dataclass, field, asdict

from .utils import EnhancedJSONEncoder


@dataclass
class IngestionPacket:
    unique_id: str
    file_to_upload: str
    ingestion_class: str
    dataset_fields: dict = field(default_factory=dict)
    scientific_metadata: dict = field(default_factory=dict)
    keywords: list = field(default_factory=list)
    samples: list = field(default_factory=list)
    children: list = field(default_factory=list)
    thumbnails: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)

    def to_json(self, path):
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, cls=EnhancedJSONEncoder, indent=4)
        return path
