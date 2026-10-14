# New-Upload Ingestion Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let ingestion requests succeed for files uploaded through crucible-api's new signed-upload flow (opaque `_files/{upload_id}` object keys, no extension) on both staging and prod, without breaking any existing legacy-upload ingestion.

**Architecture:** crucible-api sends the RMQ ingestion message with two name-like fields: `filename` (full `bucket/object` path, used only to locate bytes) and `file_name` (the record's display filename, may contain relative folders, used only to classify/label). Today `crucible_ingestion` conflates these into one `file_to_upload` string that is both the open-path and the classification key. We split that: every ingestor's `is_file_supported()` and the base class's name/format derivation switch from reading `self.file_to_upload` to reading a new `self.match_name` property (`file_name` when given, else `file_to_upload` — so legacy messages, which never send `file_name`, are unaffected). `ingestion-cloud-consumer` gets a bucket→mount mapping read from config/env (not hardcoded), covering `mf-storage-staging` in addition to the existing `mf-storage-prod` entry, and passes `file_name` through; `storage_generation` is accepted in the message but intentionally not acted on yet, since multipart objects are immutable and never overwritten. A new GCS-FUSE PV/PVC must be mounted for `mf-storage-staging` in the staging deployment only (bucket and IAM already exist; only the k8s volume is missing); staging keeps its existing `mf-storage-prod` mount too, since existing staging records still point there.

**Tech Stack:** Python 3.11, pydantic v2 (via `crucible.models.Dataset`), pytest, h5py/numpy for ingestor tests, Kubernetes (GKE) + gcsfuse CSI driver, Google Cloud Storage, RabbitMQ (pika).

**Spec:** This plan's spec is this conversation's findings plus the crucible-api session's explicit requirements, summarized here since there is no separate written spec document:
- crucible-api is moving to one storage bucket per environment: staging → `mf-storage-staging` (already created; cluster workload identity and the multipart signer already have `objectAdmin`), prod → `mf-storage-prod`. Both legacy and new multipart uploads share that one bucket per environment.
- New multipart objects live at `{dataset_mfid}/_files/{upload_id}` — opaque keys, no name, no extension, and these objects are never overwritten (so `storage_generation` doesn't need a staleness check — see Task 5). Legacy objects stay at `{dataset_mfid}/{filename}` in the same bucket.
- Existing staging dataset records keep pointing at `mf-storage-prod`, and staging must keep being able to ingest those — so staging keeps both buckets mounted.
- Prod will later get multipart enabled too, writing `_files/{upload_id}` keys into `mf-storage-prod` — the bucket→mount mapping and `match_name` logic must already support that (neither is bucket-specific), so no further code change is anticipated for that rollout.
- The RMQ ingestion message fields are: `filename` (full `bucket/object` path), `storage_generation` (multipart files only, currently unused), `file_name` (display filename, may contain relative folders), `reqid`, `dsid`, `ingestion_class`.
- crucible-api is implementing its side now (`UPLOAD_BUCKET`, `STORAGE_READ_BUCKETS`, staging configmap → `mf-storage-staging`) and will not request ingestion for new-style staging uploads until this consumer is confirmed deployed; their user is the only staging user, so there's no cross-blocking on timing.
- Confirmed live in this session: `gs://mf-storage-staging` bucket exists and the cluster's workload-identity SA already has `storage.objectAdmin` on it (same role it has on `mf-storage-prod`). No PV/PVC for it exists yet, and neither `k8s-conf-staging.yaml` nor `k8s-conf-main.yaml` mounts it.
- Audited the other call sites crucible-api flagged (`czi_ingestor.py:89`, `bcf_ingestor.py:24`, `scope_foundry_ingestors.py:214,515`, `data_ingestion.py:66`): `czi_ingestor.py:89` and `bcf_ingestor.py:24` build a thumbnail's *output* filename from `os.path.basename(self.file_to_upload)` and immediately write-then-read it back in the same method — self-consistent, unaffected by an opaque basename, no change needed. `scope_foundry_ingestors.py:214` (`CanonCaptureScopeFoundryH5Ingestor`) and `:515` (`SpinbotCameraCaptureIngestor`) instead read a genuine pre-existing *sibling* file (`{file_to_upload}.JPG` / `{basename}.{jpg,tif}`) deposited on the mount by ScopeFoundry's own uploader convention — unrelated to crucible-api's signed multipart flow unless those two instruments' uploader itself changes to use it, which isn't indicated. No change planned for either; flagged as a pre-existing limitation to revisit only if that uploader path changes. `data_ingestion.py:66` builds `associated_files` from the *existing* dataset's `list_files()` response (bookkeeping for already-ingested files), not from the file currently being classified — unaffected, no change needed.

## Global Constraints

- Legacy ingestion (no `file_name` in the message, no `_files/{upload_id}` keys) must keep working byte-for-byte identically — every existing test in `crucible-ingestion` must still pass unchanged.
- `file_name` is optional everywhere it is threaded through (`None` default) — this is what makes the change backward compatible, not a special-cased branch.
- Never use `file_name` (or `match_name`) for actual file I/O (`open()`, `h5py.File()`, `nio.mrc.fileMRC()`, `unzip`, `os.path.getsize`/`getctime`) — only `file_to_upload` is a real path on disk. `match_name` is for string classification only.
- `storage_generation` is accepted in the message but not acted on (multipart objects are immutable/never overwritten, so the live generation is always the right one) — don't build staleness-check logic for it now.
- The bucket→mount mapping in `ingestion-cloud-consumer` must be configurable (env/config-driven), not a hardcoded Python dict literal, so a new bucket can be added by deploy config alone.
- Don't touch `crucible_ingestion/ingestors/lmp_ingestor.py` — it defines `LmpIngestor` but it is not imported into `registry.py`'s `ingestor_list`, so it's dead code, out of scope.
- Don't touch `k8s-conf-main.yaml` — prod never reads `mf-storage-staging`.

## Review Focus

- A legacy message with no `file_name` must resolve to the same ingestor it does today — regression risk is silent misclassification, not a crash, so every touched ingestor needs a passing-both-ways test, not just a new one.
- A ScopeFoundry H5 variant (QSpleem SV-ramp/SPLEEM/ARRES/deposition-monitor, BioGlow, Nirvana) is claimed by a *filename suffix convention* (`_sv_ramp.h5`, `_ARRES_EK.h5`, …), not just an extension — `file_name` must carry the full original name, and a test must prove an opaque local path + a realistic `file_name` still picks the right subclass.
- A message whose `filename` starts with `mf-storage-staging` but the staging deployment's `/mnt/gcs-staging` mount doesn't exist yet must fail loudly (file-not-found path) rather than resolving to some other bucket's mount by accident.
- An unrecognized bucket prefix (a typo in config, or a bucket added on the API side before the consumer's mapping is updated) must resolve to "not found" and leave the message retryable, not silently match the wrong mount via a loose prefix check (e.g. `mf-storage-prod` must not accidentally prefix-match `mf-storage-production` or similar).
- `dataset_name`/`data_format` derived from an opaque `file_to_upload` (e.g. `dataset_name` ending up as `_files` or an upload-id string) must actually use `match_name` — this is a different code path from `is_file_supported()` and is easy to miss when the review focus is only on the registry scan.

---

### Task 1: `match_name` on the base ingestor + threading `file_name` through parse/registry/CLI

**Files:**
- Modify: `crucible_ingestion/ingestors/crucible_ingestor.py:1-9` (imports), `:26-41` (class body / `is_file_supported`), `:77-83` (`parse_dataset_name`), `:121-144` (`get_dataset_metadata`)
- Modify: `crucible_ingestion/ingestors/registry.py:102-126` (`find_supported_ingestor`)
- Modify: `crucible_ingestion/data_ingestion.py:71-142` (`parse`), `:304-327` (`data_ingestion`)
- Modify: `crucible_ingestion/cli.py:18-56`
- Test: `tests/test_match_name.py` (new)

**Interfaces:**
- Produces: `CrucibleDatasetIngestor.file_name: Optional[str]` (new pydantic field, default `None`), `CrucibleDatasetIngestor.match_name` (property, `str`, returns `self.file_name or self.file_to_upload`).
- Produces: `find_supported_ingestor(dataset_to_process, dsid, specified_ingestor=None, ingestor_list=ingestor_list, file_name=None)` — unchanged return `(ig, ingestion_class_name)`.
- Produces: `parse(dataset_to_process, dsid, ingestion_class=None, cleanup=True, file_name=None)` and `data_ingestion(dataset_to_process, dsid, ingestion_class=None, include_file=False, file_name=None)` — unchanged return types.
- Consumes: nothing from other tasks (this is the foundation every other task builds on).

- [ ] **Step 1: Write the failing tests for `match_name` and base-class derivation**

```python
# tests/test_match_name.py
"""file_name is optional everywhere; when absent, match_name must fall back to
file_to_upload so every legacy (no file_name) call path is unaffected.
"""
from pathlib import Path

from crucible_ingestion.ingestors.crucible_ingestor import CrucibleDatasetIngestor


def test_match_name_falls_back_to_file_to_upload():
    ig = CrucibleDatasetIngestor(file_to_upload="/mnt/gcs/foo/bar.h5", unique_id="x")
    assert ig.match_name == "/mnt/gcs/foo/bar.h5"


def test_match_name_prefers_file_name_when_given():
    ig = CrucibleDatasetIngestor(
        file_to_upload="/mnt/gcs-staging/MF123/_files/abcd1234",
        file_name="scan_001.h5",
        unique_id="x",
    )
    assert ig.match_name == "scan_001.h5"


def test_dataset_name_derived_from_match_name_not_opaque_path(tmp_path):
    opaque = tmp_path / "abcd1234"
    opaque.write_bytes(b"data")
    ig = CrucibleDatasetIngestor(
        file_to_upload=str(opaque), file_name="scan_001.h5", unique_id="x"
    )
    ig.parse_dataset_name()
    assert ig.dataset_name == "scan_001.h5"


def test_data_format_derived_from_match_name_not_opaque_path(tmp_path):
    opaque = tmp_path / "abcd1234"
    opaque.write_bytes(b"data")
    ig = CrucibleDatasetIngestor(
        file_to_upload=str(opaque), file_name="scan_001.h5", unique_id="x"
    )
    ig.get_dataset_metadata()
    assert ig.data_format == "h5"


def test_data_format_still_derived_from_file_to_upload_when_no_file_name(tmp_path):
    legacy = tmp_path / "scan_001.h5"
    legacy.write_bytes(b"data")
    ig = CrucibleDatasetIngestor(file_to_upload=str(legacy), unique_id="x")
    ig.get_dataset_metadata()
    assert ig.data_format == "h5"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_match_name.py -v`
Expected: FAIL — `file_name` is not a recognized field / `match_name` does not exist yet (first two tests error; the rest currently pass by coincidence only for the no-`file_name` legacy case, so expect at least the first three to fail).

- [ ] **Step 3: Add `file_name`/`match_name` to the base class**

In `crucible_ingestion/ingestors/crucible_ingestor.py`, add the import and field/property:

```python
# at the top, alongside the other imports
from typing import Optional
```

```python
class CrucibleDatasetIngestor(Dataset):
    ingestion_githash: str = os.environ.get("GITHASH")
    scientific_metadata: dict = {} 
    keywords: list = []
    acl: list = []
    associated_files: dict = {} 
    thumbnails: list = []
    samples: list = []
    children: list = []
    file_name: Optional[str] = None

    @property
    def match_name(self):
        """The name to classify/label this file by.

        file_to_upload is where the bytes actually live (opaque for new-style
        signed uploads); file_name, when given, is the original name with
        extension. Never use this for I/O.
        """
        return self.file_name or self.file_to_upload

    @property
    def ingestion_class(self):
        return type(self).__name__

    def is_file_supported(self):
        return True
```

- [ ] **Step 4: Point name/format derivation at `match_name`**

In `parse_dataset_name`:

```python
    def parse_dataset_name(self):
        if self.dataset_name:
            return
        else:
            self.dataset_name = Path(self.match_name).name
            logger.info(f"{self.dataset_name=}")
            return
```

In `get_dataset_metadata`, change only the `data_format` line (leave `os.path.getsize`/`getctime` on `file_to_upload` — those are real I/O):

```python
        self.size += os.path.getsize(self.file_to_upload)
        if not self.data_format:
            self.data_format = self.match_name.split('.')[-1]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_match_name.py -v`
Expected: PASS (all 5 tests)

- [ ] **Step 6: Thread `file_name` through the registry scan**

In `crucible_ingestion/ingestors/registry.py`, update the signature and both construction sites:

```python
def find_supported_ingestor(dataset_to_process,
                            dsid,
                            specified_ingestor = None,
                            ingestor_list = ingestor_list,
                            file_name = None):
    
    if specified_ingestor is not None:
        cls = SELECTABLE.get(specified_ingestor)
        if cls is None:
            logger.warning(f"Specified ingestor '{specified_ingestor}' not found, falling back to list scan")
        else:
            logger.info(cls)
            ig = cls(file_to_upload = dataset_to_process, unique_id = dsid, file_name = file_name)
            if ig.is_file_supported():
                logger.info(f"{dataset_to_process} is supported by {specified_ingestor}")
                return ig, specified_ingestor
            else:
                logger.warning(f"{dataset_to_process} not supported by {specified_ingestor}")

    # if that ingestor class was not supported, check the others
    for ingestor_class in ingestor_list:
        ig = ingestor_class(file_to_upload = dataset_to_process, unique_id = dsid, file_name = file_name)

        if ig.is_file_supported():
            logger.info(f"{dataset_to_process} is supported by {ingestor_class.__name__}")
            return ig, ingestor_class.__name__
        else:
            continue

    return None, None
```

- [ ] **Step 7: Thread `file_name` through `parse()` and `data_ingestion()`**

In `crucible_ingestion/data_ingestion.py`, update both signatures:

```python
def parse(dataset_to_process, dsid, ingestion_class=None, cleanup=True, file_name=None):
    """Parse a file into an IngestionPacket, or None if no ingestor supports it.

    Args:
        cleanup: Remove TMP_DIR before returning. Pass False when the caller still
            needs the child files the ingestor extracted there.
        file_name: The original file name (with extension), when the real read
            path (dataset_to_process) is an opaque storage key that carries no
            name of its own. Optional; omit for legacy callers.
    """
    logger.info("running build packet...")
    ig, ingestion_class = find_supported_ingestor(dataset_to_process, dsid, ingestion_class,
                                                   file_name=file_name)
```

(rest of `parse()` is unchanged)

```python
def data_ingestion(dataset_to_process, dsid, ingestion_class=None, include_file = False, file_name=None):
    """Parse and push a file.

    Returns:
        tuple: (dataset, ingestion_class, supported). `supported` is False when no
            ingestor claimed the file; with include_file=True the file is still
            uploaded, so `dataset` may be non-None even then.
    """
    packet = parse(dataset_to_process, dsid, ingestion_class, cleanup=False, file_name=file_name)
```

(rest of `data_ingestion()` is unchanged)

- [ ] **Step 8: Add a CLI flag for local testing against opaque paths**

In `crucible_ingestion/cli.py`, add the argument and thread it through:

```python
    parser.add_argument("--file-name", dest="file_name", default=None,
                        help="original file name, when --file is an opaque storage "
                             "key with no name/extension of its own")
```

```python
    # parse
    packet = parse(args.file, dsid, args.ingestor, file_name=args.file_name)
```

- [ ] **Step 9: Write a registry-level test proving auto-detect works on an opaque path**

```python
# append to tests/test_match_name.py
from crucible_ingestion.ingestors.registry import find_supported_ingestor
from crucible_ingestion.ingestors.mrc_txt_ingestor import MrcTxtIngestor


def test_registry_auto_detects_opaque_path_via_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("Date/Time: 01/01/26 00:00:00\n---------------\n")
    ig, cls_name = find_supported_ingestor(str(opaque), "dsid", file_name="run.txt")
    assert cls_name == "MrcTxtIngestor"
    assert isinstance(ig, MrcTxtIngestor)


def test_registry_still_auto_detects_legacy_path_without_file_name(tmp_path):
    legacy = tmp_path / "run.txt"
    legacy.write_text("Date/Time: 01/01/26 00:00:00\n---------------\n")
    ig, cls_name = find_supported_ingestor(str(legacy), "dsid")
    assert cls_name == "MrcTxtIngestor"
```

- [ ] **Step 10: Run the full test suite**

Run: `uv run pytest -v`
Expected: PASS — all tests, including every pre-existing test file, pass unchanged.

- [ ] **Step 11: Commit**

```bash
git add crucible_ingestion/ingestors/crucible_ingestor.py \
        crucible_ingestion/ingestors/registry.py \
        crucible_ingestion/data_ingestion.py \
        crucible_ingestion/cli.py \
        tests/test_match_name.py
git commit -m "feat: add match_name so classification can use file_name instead of the real read path"
```

---

### Task 2: Switch simple extension-check ingestors to `match_name`

**Files:**
- Modify: `crucible_ingestion/ingestors/bcf_ingestor.py:20`
- Modify: `crucible_ingestion/ingestors/czi_ingestor.py:35`
- Modify: `crucible_ingestion/ingestors/dm_ingestor.py:28`
- Modify: `crucible_ingestion/ingestors/jupiter_afm_ingestor.py:95`
- Modify: `crucible_ingestion/ingestors/h5_ingestor.py:8`
- Modify: `crucible_ingestion/ingestors/image_ingestor.py:19,37`
- Modify: `crucible_ingestion/ingestors/emd_velox_ingestor.py:168`
- Modify: `crucible_ingestion/ingestors/autobot_spinrun_yaml_ingestor.py:16`
- Modify: `crucible_ingestion/ingestors/rga_tey_batch_ingestor.py:53`
- Modify: `crucible_ingestion/ingestors/emi_ingestor.py:16`
- Modify: `crucible_ingestion/ingestors/ser_ingestor.py:26`
- Modify: `crucible_ingestion/ingestors/rawtlt_ingestor.py:17-18`
- Modify: `crucible_ingestion/ingestors/mrc_ingestor.py:107`
- Modify: `crucible_ingestion/ingestors/ptychography_h5_ingestor.py:17`
- Modify: `crucible_ingestion/ingestors/mrc_txt_ingestor.py:120`
- Test: `tests/test_file_name_matching_simple.py` (new)

**Interfaces:**
- Consumes: `CrucibleDatasetIngestor.match_name` from Task 1.
- Produces: nothing new consumed by later tasks (each ingestor here is already independently selectable by the registry from Task 1).

In every file below, only the classification line(s) change — any line that opens/reads the file for content sniffing (e.g. `h5py.File(self.file_to_upload, ...)`, `nio.mrc.fileMRC(self.file_to_upload)`, `_read_fei_lines(self.file_to_upload)`) stays on `self.file_to_upload` unchanged.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_file_name_matching_simple.py
"""Each of these ingestors claims files by extension/suffix. With the new
signed-upload flow the real path is an opaque key and the extension only
exists in file_name — these tests pin that file_name-based classification
works, and that the legacy (file_name absent) path still works too.
"""
from crucible_ingestion.ingestors.bcf_ingestor import BcfIngestor
from crucible_ingestion.ingestors.h5_ingestor import H5Ingestor
from crucible_ingestion.ingestors.emi_ingestor import EmiIngestor
from crucible_ingestion.ingestors.mrc_ingestor import MrcIngestor
from crucible_ingestion.ingestors.autobot_spinrun_yaml_ingestor import SpinRunIngestor_10kLegacy
from crucible_ingestion.ingestors.rga_tey_batch_ingestor import RgaTeyBatchIngestor


def test_h5_ingestor_claims_opaque_path_via_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    ig = H5Ingestor(file_to_upload=str(opaque), file_name="scan.h5", unique_id="x")
    assert ig.is_file_supported() is True


def test_h5_ingestor_rejects_opaque_path_with_mismatched_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    ig = H5Ingestor(file_to_upload=str(opaque), file_name="scan.mpt", unique_id="x")
    assert ig.is_file_supported() is False


def test_h5_ingestor_still_claims_legacy_path_without_file_name(tmp_path):
    legacy = tmp_path / "scan.h5"
    legacy.touch()
    assert H5Ingestor(file_to_upload=str(legacy), unique_id="x").is_file_supported() is True


def test_emi_ingestor_uses_file_name_suffix(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    assert EmiIngestor(file_to_upload=str(opaque), file_name="run.emi", unique_id="x").is_file_supported() is True


def test_rga_tey_batch_checks_both_extension_and_substring_on_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    ig = RgaTeyBatchIngestor(file_to_upload=str(opaque), file_name="10k_RGA_batch.zip", unique_id="x")
    assert ig.is_file_supported() is True
    ig_wrong_name = RgaTeyBatchIngestor(file_to_upload=str(opaque), file_name="other.zip", unique_id="x")
    assert ig_wrong_name.is_file_supported() is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_file_name_matching_simple.py -v`
Expected: FAIL — all assertions involving `file_name` on an opaque path fail because `is_file_supported()` still reads `self.file_to_upload`.

- [ ] **Step 3: Apply the mechanical edits**

`bcf_ingestor.py:20`, `czi_ingestor.py:35`, `dm_ingestor.py:28`, `jupiter_afm_ingestor.py:95` (identical pattern in all four):
```python
# before
        return np.any([self.file_to_upload.endswith(ftype) for ftype in self.supported_filetypes])
# after
        return np.any([self.match_name.endswith(ftype) for ftype in self.supported_filetypes])
```

`h5_ingestor.py:8`:
```python
# before
        return self.file_to_upload.endswith('h5')
# after
        return self.match_name.endswith('h5')
```

`image_ingestor.py:19` and `:37` (both classes, identical pattern):
```python
# before
        res= np.any([self.file_to_upload.lower().endswith(imformat) for imformat in supported_image_formats])
# after
        res= np.any([self.match_name.lower().endswith(imformat) for imformat in supported_image_formats])
```
(line 37's class has no `res=`/logger wrapper — just `return np.any([self.file_to_upload...` → `return np.any([self.match_name...`)

`emd_velox_ingestor.py:168`:
```python
# before
        if not self.file_to_upload.endswith('.emd'):
# after
        if not self.match_name.endswith('.emd'):
```

`autobot_spinrun_yaml_ingestor.py:16`:
```python
# before
        if not self.file_to_upload.endswith('yaml'):
# after
        if not self.match_name.endswith('yaml'):
```

`rga_tey_batch_ingestor.py:53`:
```python
# before
        conditions = [self.file_to_upload.endswith('zip'), '10k_RGA' in self.file_to_upload]
# after
        conditions = [self.match_name.endswith('zip'), '10k_RGA' in self.match_name]
```

`emi_ingestor.py:16`:
```python
# before
        return Path(self.file_to_upload).suffix.lower() in self.supported_filetypes
# after
        return Path(self.match_name).suffix.lower() in self.supported_filetypes
```

`ser_ingestor.py:26`:
```python
# before
        file_extension = Path(self.file_to_upload).suffix.lower()
# after
        file_extension = Path(self.match_name).suffix.lower()
```

`rawtlt_ingestor.py:17-18`:
```python
# before
        file_to_upload = getattr(self, "file_to_upload")
        return Path(file_to_upload).suffix.lower() in self.supported_filetypes
# after
        return Path(self.match_name).suffix.lower() in self.supported_filetypes
```

`mrc_ingestor.py:107`:
```python
# before
        if not Path(self.file_to_upload).suffix == '.mrc':
# after
        if not Path(self.match_name).suffix == '.mrc':
```

`ptychography_h5_ingestor.py:17`:
```python
# before
        if not self.file_to_upload.endswith('_master.h5'):
# after
        if not self.match_name.endswith('_master.h5'):
```

`mrc_txt_ingestor.py:120`:
```python
# before
        has_ending = np.any([self.file_to_upload.endswith(ftype)
                       for ftype in self.supported_filetypes])
# after
        has_ending = np.any([self.match_name.endswith(ftype)
                       for ftype in self.supported_filetypes])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_file_name_matching_simple.py -v`
Expected: PASS

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -v`
Expected: PASS — in particular `tests/test_mrc_txt_is_file_supported.py` must still pass unchanged (it never sets `file_name`, pinning the legacy path).

- [ ] **Step 6: Commit**

```bash
git add crucible_ingestion/ingestors/bcf_ingestor.py \
        crucible_ingestion/ingestors/czi_ingestor.py \
        crucible_ingestion/ingestors/dm_ingestor.py \
        crucible_ingestion/ingestors/jupiter_afm_ingestor.py \
        crucible_ingestion/ingestors/h5_ingestor.py \
        crucible_ingestion/ingestors/image_ingestor.py \
        crucible_ingestion/ingestors/emd_velox_ingestor.py \
        crucible_ingestion/ingestors/autobot_spinrun_yaml_ingestor.py \
        crucible_ingestion/ingestors/rga_tey_batch_ingestor.py \
        crucible_ingestion/ingestors/emi_ingestor.py \
        crucible_ingestion/ingestors/ser_ingestor.py \
        crucible_ingestion/ingestors/rawtlt_ingestor.py \
        crucible_ingestion/ingestors/mrc_ingestor.py \
        crucible_ingestion/ingestors/ptychography_h5_ingestor.py \
        crucible_ingestion/ingestors/mrc_txt_ingestor.py \
        tests/test_file_name_matching_simple.py
git commit -m "feat: classify extension-based ingestors by match_name, not the real read path"
```

---

### Task 3: Switch content-sniffing ingestors to `match_name`

**Files:**
- Modify: `crucible_ingestion/ingestors/biologic_mpt_ingestor.py:53`
- Modify: `crucible_ingestion/ingestors/gc_log_ingestor.py:26`
- Modify: `crucible_ingestion/ingestors/inorganic_xrd_ingestor.py:17`
- Modify: `crucible_ingestion/ingestors/emd_ingestor.py:19`
- Modify: `crucible_ingestion/ingestors/insitu_pl_ingestor.py:31`
- Test: `tests/test_file_name_matching_content_sniffing.py` (new)

**Interfaces:**
- Consumes: `CrucibleDatasetIngestor.match_name` from Task 1.
- Produces: nothing consumed by later tasks.

These five all run real I/O (`open()`, `nio.emd.fileEMD()`, `unzip`) *after* the extension check, inside the same method — the edit must touch only the extension-check line, never the line(s) that follow.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_file_name_matching_content_sniffing.py
from crucible_ingestion.ingestors.biologic_mpt_ingestor import BiologicMptIngestor
from crucible_ingestion.ingestors.gc_log_ingestor import GCLogIngestor
from crucible_ingestion.ingestors.inorganic_xrd_ingestor import InorganicXRDIngestor


def test_biologic_mpt_rejects_opaque_path_with_wrong_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("whatever")
    ig = BiologicMptIngestor(file_to_upload=str(opaque), file_name="run.h5", unique_id="x")
    assert ig.is_file_supported() is False


def test_gc_log_rejects_opaque_path_without_matching_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("a\tb\tc\td\n")
    ig = GCLogIngestor(file_to_upload=str(opaque), file_name="run.txt", unique_id="x")
    assert ig.is_file_supported() is False


def test_inorganic_xrd_accepts_opaque_path_via_file_name_and_still_sniffs_content(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("header\nIntensity, cps\n")
    ig = InorganicXRDIngestor(file_to_upload=str(opaque), file_name="scan.txt", unique_id="x")
    assert ig.is_file_supported() is True
    bad_content = tmp_path / "f3a9c1e1"
    bad_content.write_text("header\nnot the right second line\n")
    ig_bad = InorganicXRDIngestor(file_to_upload=str(bad_content), file_name="scan.txt", unique_id="x")
    assert ig_bad.is_file_supported() is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_file_name_matching_content_sniffing.py -v`
Expected: FAIL — extension checks still read `self.file_to_upload`, so an opaque path (no `.log`/`.mpt`/`.txt` suffix) is rejected regardless of `file_name`, flipping the expected results above.

- [ ] **Step 3: Apply the mechanical edits (extension-check line only)**

`biologic_mpt_ingestor.py:53`:
```python
# before
        if not self.file_to_upload.lower().endswith('.mpt'):
# after
        if not self.match_name.lower().endswith('.mpt'):
```

`gc_log_ingestor.py:26`:
```python
# before
        if not self.file_to_upload.lower().endswith('.log'):
# after
        if not self.match_name.lower().endswith('.log'):
```

`inorganic_xrd_ingestor.py:17`:
```python
# before
        if not self.file_to_upload.endswith('.txt'):
# after
        if not self.match_name.endswith('.txt'):
```

`emd_ingestor.py:19` (BerkeleyEmdIngestor):
```python
# before
        if not self.file_to_upload.endswith('.emd'):
# after
        if not self.match_name.endswith('.emd'):
```

`insitu_pl_ingestor.py:31`:
```python
# before
        if all([self.file_to_upload.endswith('zip')]):
# after
        if all([self.match_name.endswith('zip')]):
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_file_name_matching_content_sniffing.py -v`
Expected: PASS

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crucible_ingestion/ingestors/biologic_mpt_ingestor.py \
        crucible_ingestion/ingestors/gc_log_ingestor.py \
        crucible_ingestion/ingestors/inorganic_xrd_ingestor.py \
        crucible_ingestion/ingestors/emd_ingestor.py \
        crucible_ingestion/ingestors/insitu_pl_ingestor.py \
        tests/test_file_name_matching_content_sniffing.py
git commit -m "feat: classify content-sniffing ingestors by match_name before reading bytes"
```

---

### Task 4: Switch ScopeFoundry H5 family to `match_name`

**Files:**
- Modify: `crucible_ingestion/ingestors/scope_foundry_ingestors.py:78,675,732,764,848,915,976,1247,1559,1648,1694` (exact line numbers before this task's edits; re-check with `grep -n "def is_file_supported" -A1` since earlier tasks don't touch this file)
- Test: `tests/test_scope_foundry_matching.py` (extend existing file)

**Interfaces:**
- Consumes: `CrucibleDatasetIngestor.match_name` from Task 1.
- Produces: nothing consumed by later tasks.

This is the family flagged in Review Focus: several of these classify by a *filename suffix convention* (e.g. `_sv_ramp.h5`, `_ARRES_EK.h5`), not a bare extension, so `file_name` must carry the full original name for these to keep working on opaque paths.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_scope_foundry_matching.py
from crucible_ingestion.ingestors.scope_foundry_ingestors import (
    QSpleemSVRampIngestor,
    QSpleemARRESEKIngestor,
    BioGlowIngestor,
)


def test_qspleem_sv_ramp_claims_opaque_path_via_file_name(tmp_path):
    path = scopefoundry_file(tmp_path, 'sv_ramp')
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_bytes(open(path, 'rb').read())
    ig = QSpleemSVRampIngestor(file_to_upload=str(opaque), file_name="run_sv_ramp.h5", unique_id="x")
    assert ig.is_file_supported() is True


def test_qspleem_arres_ek_regex_matches_against_file_name_not_opaque_path(tmp_path):
    path = scopefoundry_file(tmp_path, 'ARRES_EK')
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_bytes(open(path, 'rb').read())
    ig = QSpleemARRESEKIngestor(file_to_upload=str(opaque), file_name="run_ARRES_EK_images_0000.h5", unique_id="x")
    assert ig.is_file_supported() is True
    ig_no_name = QSpleemARRESEKIngestor(file_to_upload=str(opaque), unique_id="x")
    assert ig_no_name.is_file_supported() is False


def test_bioglow_still_claims_legacy_path_without_file_name(tmp_path):
    path = scopefoundry_file(tmp_path, 'bioglow_spec')
    legacy = tmp_path / "run_bioglow_spec.h5"
    legacy.write_bytes(open(path, 'rb').read())
    ig = BioGlowIngestor(file_to_upload=str(legacy), unique_id="x")
    assert ig.is_file_supported() is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scope_foundry_matching.py -v`
Expected: FAIL on the two new opaque-path tests (the suffix checks still read `self.file_to_upload`); the legacy BioGlow test passes already (not a regression check yet, just confirms the starting state).

- [ ] **Step 3: Apply the mechanical edits**

Base class (`ScopeFoundryH5Ingestor.is_file_supported`):
```python
# before
    def is_file_supported(self):
        if not self.file_to_upload.endswith('.h5'):
            return False
        meas = self.h5_measurement_name
# after
    def is_file_supported(self):
        if not self.match_name.endswith('.h5'):
            return False
        meas = self.h5_measurement_name
```
(`h5_measurement_name` itself stays on `self.file_to_upload` — it's real I/O, `h5py.File(self.file_to_upload, 'r')`.)

`BioGlowIngestor`:
```python
# before
    def is_file_supported(self):
        return self.file_to_upload.endswith('_bioglow_spec.h5')  
# after
    def is_file_supported(self):
        return self.match_name.endswith('_bioglow_spec.h5')  
```

`QSpleemImageIngestor`:
```python
# before
    def is_file_supported(self):
        return(self.file_to_upload.endswith('_image_save.h5'))
# after
    def is_file_supported(self):
        return(self.match_name.endswith('_image_save.h5'))
```

`QSpleemSVRampIngestor`:
```python
# before
    def is_file_supported(self):
        return self.file_to_upload.endswith('_sv_ramp.h5')
# after
    def is_file_supported(self):
        return self.match_name.endswith('_sv_ramp.h5')
```

`QSpleemARRESEKIngestor`:
```python
# before
        return bool(re.search(r'_ARRES_EK(_images(_\d+)?)?\.h5$', self.file_to_upload))
# after
        return bool(re.search(r'_ARRES_EK(_images(_\d+)?)?\.h5$', self.match_name))
```

`QSpleemARRESMMIngestor`:
```python
# before
        return bool(re.search(r'_ARRES_MM(_images(_\d+)?)?\.h5$', self.file_to_upload))
# after
        return bool(re.search(r'_ARRES_MM(_images(_\d+)?)?\.h5$', self.match_name))
```

`NirvanaMultiPosLineScanIngestor`:
```python
# before
        file_regex = r'.*pollux_oospec_multipos_line_scan.*\.h5'
        if re.match(file_regex, self.file_to_upload):
# after
        file_regex = r'.*pollux_oospec_multipos_line_scan.*\.h5'
        if re.match(file_regex, self.match_name):
```

`NirvanaMultiPosSpecRunIngestor`:
```python
# before
        return bool(re.match(r'.*pollux_multipos_spec_run.*\.h5', self.file_to_upload))
# after
        return bool(re.match(r'.*pollux_multipos_spec_run.*\.h5', self.match_name))
```

`QSpleemSVRampSpinIngestor`:
```python
# before
        return self.file_to_upload.endswith('_sv_ramp_spin.h5')
# after
        return self.match_name.endswith('_sv_ramp_spin.h5')
```

`QSpleemSPLEEMImageIngestor`:
```python
# before
        return self.file_to_upload.endswith('_SPLEEM_image.h5')
# after
        return self.match_name.endswith('_SPLEEM_image.h5')
```

`QSpleemDepositionMonitorIngestor`:
```python
# before
        return self.file_to_upload.endswith('_deposition_monitor.h5')
# after
        return self.match_name.endswith('_deposition_monitor.h5')
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_scope_foundry_matching.py -v`
Expected: PASS (all tests, old and new)

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crucible_ingestion/ingestors/scope_foundry_ingestors.py tests/test_scope_foundry_matching.py
git commit -m "feat: classify ScopeFoundry H5 ingestors by match_name, including filename-suffix variants"
```

---

### Task 5: `ingestion-cloud-consumer` — config-driven bucket mount mapping, `file_name` passthrough

**Files:**
- Modify: `ingestion_consumer/ingestion_process.py` (all of it — see below for exact replacement blocks)
- Modify: `k8s-conf-staging.yaml` (add the `GCS_BUCKET_MOUNTS` env var — see Step 3b)
- Test: `tests/test_path_resolution.py` (new; this repo currently has no `tests/` directory)
- Modify: `pyproject.toml` (add `pytest` as a dev dependency if not already present)

**Interfaces:**
- Consumes: `data_ingestion(dataset_to_process, dsid, ingestion_class=None, include_file=False, file_name=None)` from Task 1 (released and pinned — see Task 7).
- Produces: `resolve_bucket_and_mount(filename: str) -> tuple[str | None, str | None]` (bucket name, local mount path), `resolve_dataset_path(filename: str) -> str | None` — pulled out of `callback()` so they're unit-testable without RabbitMQ/GCS/Crucible credentials.

This task is scoped to the consumer file only; it does **not** bump the `crucible-ingestion` pin yet (that's Task 7, after Tasks 1-4 are merged and the staging mount exists per Task 6). `storage_generation` is read from the message's dict for forward compatibility but intentionally not compared against anything — per crucible-api, multipart objects are immutable and never overwritten, so there's no staleness to check yet.

- [ ] **Step 1: Write the failing tests for path/bucket resolution**

```python
# tests/test_path_resolution.py
"""Path resolution must handle both the legacy FUSE-mounted multipart bucket and
the new per-environment buckets, driven entirely by the GCS_BUCKET_MOUNTS env
var (not a hardcoded mapping), and must fail loudly (None) rather than
guessing when it sees something it doesn't recognize.
"""
import json

import pytest


@pytest.fixture
def bucket_mounts(monkeypatch):
    monkeypatch.setenv(
        "GCS_BUCKET_MOUNTS",
        json.dumps({"mf-storage-prod": "/mnt/gcs-prod", "mf-storage-staging": "/mnt/gcs-staging"}),
    )
    import importlib
    import ingestion_consumer.ingestion_process as mod
    importlib.reload(mod)
    return mod


def test_legacy_mnt_gcs_path_passes_through(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("/mnt/gcs/foo/bar.h5") == "/mnt/gcs/foo/bar.h5"


def test_legacy_crucible_uploads_prefix_maps_to_mnt_gcs(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("crucible-uploads/foo/bar.h5") == "/mnt/gcs/foo/bar.h5"


def test_prod_bucket_prefix_maps_to_configured_mount(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("mf-storage-prod/MF123/_files/abcd1234") == "/mnt/gcs-prod/MF123/_files/abcd1234"


def test_staging_bucket_prefix_maps_to_configured_mount(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("mf-storage-staging/MF123/_files/abcd1234") == "/mnt/gcs-staging/MF123/_files/abcd1234"


def test_legacy_object_key_under_shared_bucket_also_resolves(bucket_mounts):
    # Legacy-named objects now live in the same per-environment bucket as new ones.
    assert bucket_mounts.resolve_dataset_path("mf-storage-prod/MF123/original_name.h5") == "/mnt/gcs-prod/MF123/original_name.h5"


def test_unrecognized_prefix_returns_none(bucket_mounts):
    assert bucket_mounts.resolve_dataset_path("some-other-bucket/foo") is None


def test_bucket_prefix_match_does_not_loosely_match_similar_names(bucket_mounts):
    # mf-storage-prod must not prefix-match an unconfigured lookalike bucket.
    assert bucket_mounts.resolve_dataset_path("mf-storage-production/foo") is None


def test_resolve_bucket_and_mount_identifies_staging_bucket(bucket_mounts):
    bucket, mount = bucket_mounts.resolve_bucket_and_mount("mf-storage-staging/MF123/_files/abcd1234")
    assert bucket == "mf-storage-staging"
    assert mount == "/mnt/gcs-staging"


def test_resolve_bucket_and_mount_returns_none_for_legacy_paths(bucket_mounts):
    bucket, mount = bucket_mounts.resolve_bucket_and_mount("/mnt/gcs/foo/bar.h5")
    assert bucket is None


def test_missing_env_var_falls_back_to_known_defaults(monkeypatch):
    # Deploys that haven't set GCS_BUCKET_MOUNTS yet must keep working exactly
    # as before (mf-storage-prod -> /mnt/gcs-prod), so the rollout order
    # (code first, env var second) can't create a window with no mapping at all.
    monkeypatch.delenv("GCS_BUCKET_MOUNTS", raising=False)
    import importlib
    import ingestion_consumer.ingestion_process as mod
    importlib.reload(mod)
    assert mod.resolve_dataset_path("mf-storage-prod/MF123/foo.h5") == "/mnt/gcs-prod/MF123/foo.h5"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_path_resolution.py -v`
Expected: FAIL — `resolve_bucket_and_mount`/`resolve_dataset_path` don't exist yet (all logic is inline in `callback()`), and there's no `GCS_BUCKET_MOUNTS` handling at all.

- [ ] **Step 3: Replace `ingestion_process.py`'s bucket handling with the config-driven version**

Replace the whole file's bucket/path section (everything from the `_GCS_PROD_BUCKET` constant through the end of `is_file_lost`, plus the path-resolution block inside `callback`) with:

```python
import os
import json
import time
import logging
import requests
from google.cloud import storage as gcs
from google.api_core.exceptions import NotFound

from crucible import CrucibleClient
from crucible.utils.io import get_tz_isoformat
from crucible_ingestion import data_ingestion, set_client
from crucible_ingestion.utils import get_ingestion_githash
from .cloud import get_secret, setup_pika_client

# Buckets that are GCS-FUSE mounted read-only and also checked directly via the
# GCS client to bypass FUSE cache latency. Configured via GCS_BUCKET_MOUNTS (a
# JSON object of {bucket_name: mount_path}) so a new bucket is a deploy-config
# change, not a code change; the literal below is only the fallback for a
# deploy that hasn't set the env var yet.
_DEFAULT_BUCKET_MOUNTS = {
    "mf-storage-prod": "/mnt/gcs-prod",
}


def _load_bucket_mounts():
    raw = os.environ.get("GCS_BUCKET_MOUNTS")
    if not raw:
        return dict(_DEFAULT_BUCKET_MOUNTS)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logging.getLogger(__name__).error(
            f"GCS_BUCKET_MOUNTS is not valid JSON ({raw!r}), falling back to defaults"
        )
        return dict(_DEFAULT_BUCKET_MOUNTS)


_BUCKET_MOUNTS = _load_bucket_mounts()

logger = logging.getLogger(__name__)

ingestion_githash = get_ingestion_githash()
num_cores = os.cpu_count()

# RMQ Setup ===========================
rmq_pw = get_secret("RABBITMQ_DEFAULT_PW", "rabbitmq_default_pw/versions/1")
rmq_host = os.environ.get('RMQ_HOST')
rmq_port = os.environ.get('RMQ_PORT')
RMQ_ROUTING_SUFFIX = os.environ.get('RMQ_ROUTING_SUFFIX')

connection, channel = setup_pika_client(rmq_host, rmq_port, rmq_pw)

queues_needed = [f'ingestion-{RMQ_ROUTING_SUFFIX}', 'not-supported', f'ingestion-{RMQ_ROUTING_SUFFIX}-failed']

for q in queues_needed:
    channel.queue_declare(queue=q)

# Crucible Setup ===========================
crucible_api_url = os.environ.get('CRUCIBLE_API_URL')
crucible_apikey = get_secret("CRUCIBLE_APIKEY", "crucible_admin_apikey/versions/4")
client = CrucibleClient(api_url=crucible_api_url, api_key=crucible_apikey)
set_client(client)


# Functions ===========================
def resolve_bucket_and_mount(filename):
    """(bucket_name, mount_path) if filename names an object in one of the
    configured buckets, else (None, None). Prefix match is anchored on a
    trailing '/' so 'mf-storage-prod' can't loosely match
    'mf-storage-production/...'."""
    for bucket, mount in _BUCKET_MOUNTS.items():
        if filename == bucket or filename.startswith(bucket + '/'):
            return bucket, mount
    return None, None


def resolve_dataset_path(filename):
    """The local FUSE-mounted path for a message's filename, or None if the
    prefix isn't recognized."""
    if filename.startswith('/mnt/gcs'):
        return filename
    if filename.startswith('crucible-uploads'):
        return filename.replace('crucible-uploads', '/mnt/gcs', 1)
    bucket, mount = resolve_bucket_and_mount(filename)
    if bucket is not None:
        return filename.replace(bucket, mount, 1)
    return None


def is_file_lost(message, dataset_to_process, ch, update_status=True):
    reqid = message['reqid']
    filename = message['filename']

    bucket_name, _ = resolve_bucket_and_mount(filename)
    if bucket_name is not None:
        # Bypass FUSE cache latency for configured buckets.
        blob_name = filename[len(bucket_name) + 1:]
        try:
            file_exists = gcs.Client().bucket(bucket_name).blob(blob_name).exists()
        except Exception as e:
            logger.warning(f"GCS client check failed for {blob_name}: {e}, falling back to os.path")
            file_exists = os.path.exists(dataset_to_process)
    else:
        file_exists = os.path.exists(dataset_to_process)

    if not file_exists:
        if update_status:
            client.ingestions.update(reqid, status="file not found")
        return True
    return False
```

Then, inside `callback()`, replace the inline path-resolution block:

```python
    # before
    filename = message['filename']
    filename = filename.replace('\\', '/')
    if filename.startswith('/mnt/gcs'):
        dataset_to_process = filename
    elif filename.startswith('crucible-uploads'):
        dataset_to_process = filename.replace('crucible-uploads', '/mnt/gcs', 1)
    elif filename.startswith('mf-storage-prod'):
        dataset_to_process = filename.replace('mf-storage-prod', '/mnt/gcs-prod', 1)
    else:
        logger.error(f"Unexpected filename format, cannot resolve path: {filename}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
```
```python
    # after
    filename = message['filename']
    filename = filename.replace('\\', '/')
    dataset_to_process = resolve_dataset_path(filename)
    if dataset_to_process is None:
        logger.error(f"Unexpected filename format, cannot resolve path: {filename}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
```

And the `data_ingestion(...)` call — pass `file_name` through; `storage_generation` is accepted in the message but deliberately not used yet:

```python
    # before
                ds, ingestion_class, supported = data_ingestion(dataset_to_process = dataset_to_process,
                                                                dsid = dsid,
                                                                ingestion_class = specified_ingestor,
                                                                include_file = False)
```
```python
    # after
                ds, ingestion_class, supported = data_ingestion(dataset_to_process = dataset_to_process,
                                                                dsid = dsid,
                                                                ingestion_class = specified_ingestor,
                                                                include_file = False,
                                                                file_name = message.get('file_name'))
```

- [ ] **Step 3b: Add `GCS_BUCKET_MOUNTS` to the staging deployment only**

In every deployment block of `k8s-conf-staging.yaml` (small/medium/large), add one entry next to the existing `CRUCIBLE_API_URL` env var:

```yaml
        - name: GCS_BUCKET_MOUNTS
          value: '{"mf-storage-prod": "/mnt/gcs-prod", "mf-storage-staging": "/mnt/gcs-staging"}'
```

Don't touch `k8s-conf-main.yaml` here: it never reads `mf-storage-staging`, and `_DEFAULT_BUCKET_MOUNTS` in the code above already matches prod's current (and only) need (`mf-storage-prod` → `/mnt/gcs-prod`) with no env var required. When prod's own multipart rollout happens, that's the point to add `GCS_BUCKET_MOUNTS` there too — not before.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_path_resolution.py -v`
Expected: PASS

- [ ] **Step 5: Add `pytest` as a dev dependency if missing, and confirm the module still imports under `python -c`**

```bash
cd ~/software/ingestion-cloud-consumer
grep -q '"pytest' pyproject.toml || uv add --dev pytest
GCS_BUCKET_MOUNTS='{"mf-storage-prod": "/mnt/gcs-prod"}' python3 -c "import ast; ast.parse(open('ingestion_consumer/ingestion_process.py').read())"
```
Expected: no syntax errors.

- [ ] **Step 6: Commit**

```bash
git add ingestion_consumer/ingestion_process.py k8s-conf-staging.yaml \
        tests/test_path_resolution.py pyproject.toml uv.lock
git commit -m "feat: make bucket->mount mapping config-driven and support mf-storage-staging, pass file_name through"
```

---

### Task 6: Mount `mf-storage-staging` in the staging deployment

**Files:**
- Create: `k8s-conf-mf-storage-staging-pv.yaml` (new, applied once, not part of the rolling deploy)
- Modify: `k8s-conf-staging.yaml` (all three deployments: small/medium/large)

**Interfaces:**
- Consumes: nothing from earlier tasks (infra-only; independent of the code changes, but ingestion for staging signed uploads cannot succeed without it).
- Produces: `/mnt/gcs-staging` mount available in the staging deployment, consumed by `resolve_bucket_and_mount`/`resolve_dataset_path` from Task 5.

Confirmed in this session: `gs://mf-storage-staging` exists, and the cluster's workload-identity SA (`principal://iam.googleapis.com/projects/776258882599/locations/global/workloadIdentityPools/mf-crucible.svc.id.goog/subject/ns/default/sa/default`) already has `roles/storage.objectAdmin` on it — no IAM change needed, only the PV/PVC/mount.

- [ ] **Step 1: Write the static PV/PVC manifest, modeled on the existing `mf-storage-prod` one**

```yaml
# k8s-conf-mf-storage-staging-pv.yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: mf-storage-staging-pv
spec:
  accessModes:
  - ReadWriteMany
  capacity:
    storage: 500Gi
  csi:
    driver: gcsfuse.csi.storage.gke.io
    volumeAttributes:
      gcsfuseLoggingSeverity: warning
    volumeHandle: mf-storage-staging
  mountOptions:
  - implicit-dirs
  storageClassName: mf-storage-staging
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mf-storage-staging-pvc
  namespace: default
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 500Gi
  storageClassName: mf-storage-staging
  volumeName: mf-storage-staging-pv
```

- [ ] **Step 2: Add the volume and mount to all three staging deployments**

In `k8s-conf-staging.yaml`, for each of the `small`/`medium`/`large` deployments, add one `volumeMounts` entry and one `volumes` entry (identical in all three):

```yaml
        volumeMounts:
        - mountPath: /mnt/gcs
          name: cruc-upload-vol2
          readOnly: true
        - mountPath: /mnt/gcs-prod
          name: mf-storage-prod-vol
          readOnly: true
        - mountPath: /mnt/gcs-staging
          name: mf-storage-staging-vol
          readOnly: true
```

```yaml
      volumes:
      - name: cruc-upload-vol2
        persistentVolumeClaim:
          claimName: cruc-upload-pvc2
          readOnly: true
      - name: mf-storage-prod-vol
        persistentVolumeClaim:
          claimName: mf-storage-prod-pvc
          readOnly: true
      - name: mf-storage-staging-vol
        persistentVolumeClaim:
          claimName: mf-storage-staging-pvc
          readOnly: true
```

- [ ] **Step 3: Validate the manifests offline**

```bash
cd ~/software/ingestion-cloud-consumer
kubectl apply --dry-run=client -f k8s-conf-mf-storage-staging-pv.yaml
kubectl apply --dry-run=client -f k8s-conf-staging.yaml
```
Expected: both report the resources would be created/configured, no validation errors.

- [ ] **Step 4: Apply to the live cluster — confirm with the user first**

This is a shared-infrastructure change (`kubectl apply` against the `crucible-cluster` GKE cluster). Stop here and get explicit confirmation before running:

```bash
kubectl apply -f k8s-conf-mf-storage-staging-pv.yaml
kubectl apply -f k8s-conf-staging.yaml
kubectl get pvc mf-storage-staging-pvc
```
Expected: `mf-storage-staging-pvc` reaches `Bound`.

- [ ] **Step 5: Commit**

```bash
git add k8s-conf-mf-storage-staging-pv.yaml k8s-conf-staging.yaml
git commit -m "infra: mount mf-storage-staging in the staging deployment"
```

---

### Task 7: Release and roll out

**Files:**
- Modify: `ingestion-cloud-consumer/uv.lock` (via `update-crucible-ingestion.sh`)

**Interfaces:**
- Consumes: Tasks 1-4 merged to `crucible-ingestion`'s default branch; Task 5 merged to `ingestion-cloud-consumer`; Task 6 applied to the cluster.
- Produces: staging and prod running the new consumer image.

- [ ] **Step 1: Merge Tasks 1-4 to `crucible-ingestion` main**

(Whatever this repo's normal PR/merge process is — out of scope to script here.)

- [ ] **Step 2: Bump the pinned `crucible-ingestion` commit**

```bash
cd ~/software/ingestion-cloud-consumer
bash update-crucible-ingestion.sh
git add uv.lock
git commit -m "chore: update crucible-ingestion"
```

- [ ] **Step 3: Push to the `staging` branch and let Cloud Build deploy it**

Cloud Build (`cloudbuild.yaml`) builds `./k8s-conf-${BRANCH_NAME}.yaml` on push, so pushing to a branch named `staging` deploys only the staging tier — confirm this is the intended trigger (branch-per-environment) before pushing, since it both builds a new image and rolls every staging pod.

- [ ] **Step 4: Verify on staging**

Ask crucible-api/nano-crucible to send one signed-upload ingestion request against the staging environment; confirm in the consumer logs (or `client.ingestions` status) that it reaches `"complete"` rather than `"not supported"` or `"file not found"`.

- [ ] **Step 5: Push to the `main` branch for prod**

Only after staging verification passes. This rolls `k8s-conf-main.yaml` — note Task 6 was staging-only, so prod continues to serve only legacy + already-existing `mf-storage-prod` paths until prod's own signed-upload traffic needs the same staging-style bucket (it already has `mf-storage-prod` mounted, so no infra task is needed there).

- [ ] **Step 6: Tell crucible-api (and nano-crucible) it's safe to request ingestion for signed/multipart uploads**

Reply to the crucible-api session with staging deploy status as requested, and to nano-crucible, once staging is verified (don't wait on prod — crucible-api's staging rollout is independent and they said they won't block on us). crucible-api can then stop forcing the legacy route for ingestion-requesting staging uploads.
