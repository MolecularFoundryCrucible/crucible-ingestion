import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from crucible_ingestion.ingestors import bcf_ingestor
from crucible_ingestion.ingestors.bcf_ingestor import BcfIngestor


class _Signal:
    def plot(self, **kwargs):
        plt.figure()
        plt.imshow([[0, 1], [1, 0]])


def test_bcf_thumbnail_writes_nothing_to_the_working_directory(tmp_path, monkeypatch):
    workdir = tmp_path / "cwd"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(bcf_ingestor.hs, "load", lambda path: [_Signal()])

    added = []
    ig = BcfIngestor(file_to_upload="sample.bcf", unique_id="x")
    monkeypatch.setattr(BcfIngestor, "add_thumbnail",
                        lambda self, image, caption, **kw: added.append(caption))

    ig.get_thumbnails()

    assert added == ["EDS Thumbnail"]
    assert list(workdir.iterdir()) == []
    assert plt.get_fignums() == []
